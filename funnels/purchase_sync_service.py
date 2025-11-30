import json
import logging
from datetime import datetime
from typing import List, Optional

import mysql.connector
from mysql.connector import MySQLConnection

from analytics.tracking import mark_certificate_purchased
from brevo.api_client import BrevoApiClient
from brevo.outbox import enqueue_brevo_sync_job
from db.selectors import get_pending_funnel_entries, get_certificate_purchase_for_entry


class PurchaseSyncService:
    """Synchronizes certificate purchases from MODX tables to funnel analytics.

    Periodically checks funnel_entries for users who haven't purchased yet,
    queries MODX certificate payment tables, and updates both analytics and
    Brevo contact attributes when purchases are detected.
    """

    def __init__(
        self,
        connection: MySQLConnection,
        brevo_client: BrevoApiClient,
        dry_run: bool = False,
    ) -> None:
        """Initializes the purchase synchronization service.

        Args:
            connection: Active MySQL database connection for reading funnel
                entries and MODX certificate tables.
            brevo_client: Brevo API client for updating contact attributes
                after purchase detection.
            dry_run: If True, no database writes or Brevo API calls are performed.
                Only read operations and logging occur.
        """
        self.connection = connection
        self.brevo_client = brevo_client
        self.dry_run = dry_run
        self.logger = logging.getLogger("funnels.purchase_sync_service")

    def sync(self, max_rows: int = 100) -> None:
        """Processes pending funnel entries to detect certificate purchases.

        Fetches entries where certificate_purchased=0, checks MODX payment tables,
        and updates both funnel_entries and Brevo contacts when purchases are found.

        Side Effects:
            - Updates funnel_entries.certificate_purchased flag.
            - Updates Brevo contact attributes (unless dry-run mode).

        Args:
            max_rows: Maximum entries to process per run. Prevents long-running
                transactions and allows incremental processing of large backlogs.
        """
        self.logger.info(
            "Starting purchase synchronization for funnel entries (limit=%s)",
            max_rows,
        )

        pending_entries = get_pending_funnel_entries(
            connection=self.connection,
            max_rows=max_rows,
        )

        self.logger.info(
            "Fetched %s pending funnel entries",
            len(pending_entries),
        )

        for entry in pending_entries:
            email, funnel_type, user_id, test_id = entry

            purchase_row = get_certificate_purchase_for_entry(
                connection=self.connection,
                email=email,
                funnel_type=funnel_type,
                user_id=user_id,
                test_id=test_id,
            )

            if purchase_row is None:
                continue

            order_id, purchased_at = purchase_row

            purchased_at_datetime = self._ensure_datetime(purchased_at)

            self.logger.info(
                "Detected certificate purchase (email=%s, funnel_type=%s, order_id=%s)",
                email,
                funnel_type,
                order_id,
            )

            if self.dry_run:
                self.logger.info(
                    "Dry run: would update database and Brevo contact for purchase (email=%s, funnel_type=%s, test_id=%s, order_id=%s)",
                    email,
                    funnel_type,
                    test_id,
                    order_id,
                )
            else:
                try:
                    self.connection.start_transaction()

                    # Get funnel_entry_id(s) that match this purchase
                    funnel_entry_ids = self._get_funnel_entry_ids(
                        email=email,
                        funnel_type=funnel_type,
                        test_id=test_id,
                    )

                    if not funnel_entry_ids:
                        self.logger.warning(
                            "No funnel entry found for purchase (email=%s, funnel_type=%s, test_id=%s)",
                            email,
                            funnel_type,
                            test_id,
                        )
                        self.connection.rollback()
                        continue

                    # Update funnel_entries to mark as purchased
                    mark_certificate_purchased(
                        connection=self.connection,
                        email=email,
                        funnel_type=funnel_type,
                        test_id=test_id,
                        purchased_at=purchased_at_datetime,
                    )

                    # Enqueue Brevo sync job for each affected funnel entry
                    payload_data = {
                        "email": email,
                        "funnel_type": funnel_type,
                        "purchased_at": purchased_at_datetime.isoformat(),
                        "attributes": {
                            "FUNNEL_TYPE": funnel_type,
                            "CERTIFICATE_PURCHASED": 1,
                            "CERTIFICATE_PURCHASED_AT": purchased_at_datetime.isoformat(),
                        },
                    }
                    payload_json = json.dumps(payload_data)

                    for funnel_entry_id in funnel_entry_ids:
                        self.logger.info(
                            "Enqueuing Brevo sync job for purchase (funnel_entry_id=%s, operation_type=update_after_purchase)",
                            funnel_entry_id,
                        )
                        enqueue_brevo_sync_job(
                            connection=self.connection,
                            funnel_entry_id=funnel_entry_id,
                            operation_type="update_after_purchase",
                            payload=payload_json,
                        )

                    self.connection.commit()

                    self.logger.info(
                        "Successfully processed purchase (email=%s, funnel_type=%s, order_id=%s)",
                        email,
                        funnel_type,
                        order_id,
                    )

                except mysql.connector.Error as e:
                    self.connection.rollback()
                    self.logger.error(
                        "Failed to process purchase (email=%s, funnel_type=%s, order_id=%s): %s",
                        email,
                        funnel_type,
                        order_id,
                        str(e),
                    )
                    raise

        self.logger.info("Purchase synchronization finished")

    def _ensure_datetime(self, value: object) -> datetime:
        """Validates that payment timestamp is a datetime object.

        MySQL connector typically returns datetime objects, but this validation
        prevents runtime crashes if database schema changes or unexpected data
        types are returned.

        Args:
            value: Payment timestamp from database query.

        Returns:
            datetime object if validation passes.

        Raises:
            ValueError: If value is not a datetime object, indicating data
                integrity issue that requires investigation.
        """
        if isinstance(value, datetime):
            return value

        raise ValueError("Unexpected purchased_at value type")

    def _get_funnel_entry_ids(
        self,
        email: str,
        funnel_type: str,
        test_id: Optional[int],
    ) -> List[int]:
        """Gets funnel entry IDs that match the given criteria.

        Args:
            email: User email address.
            funnel_type: Funnel type ('language' or 'non_language').
            test_id: Optional test ID for more specific matching.

        Returns:
            List of funnel entry IDs that match the criteria.
        """
        cursor = self.connection.cursor()

        try:
            if test_id is None:
                query = """
                SELECT id
                FROM funnel_entries
                WHERE email = %s
                  AND funnel_type = %s
                  AND certificate_purchased = 0
                """
                params = (email, funnel_type)
            else:
                query = """
                SELECT id
                FROM funnel_entries
                WHERE email = %s
                  AND funnel_type = %s
                  AND test_id = %s
                  AND certificate_purchased = 0
                """
                params = (email, funnel_type, test_id)  # type: ignore[assignment]

            cursor.execute(query, params)
            rows = cursor.fetchall()

            return [int(row[0]) for row in rows]  # type: ignore[arg-type]

        finally:
            cursor.close()
