import logging
from datetime import datetime

from mysql.connector import MySQLConnection

from analytics.tracking import mark_certificate_purchased
from brevo.api_client import BrevoApiClient
from brevo.models import BrevoContact
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
    ) -> None:
        """Initializes the purchase synchronization service.

        Args:
            connection: Active MySQL database connection for reading funnel
                entries and MODX certificate tables.
            brevo_client: Brevo API client for updating contact attributes
                after purchase detection.
        """
        self.connection = connection
        self.brevo_client = brevo_client
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

            mark_certificate_purchased(
                connection=self.connection,
                email=email,
                funnel_type=funnel_type,
                test_id=test_id,
                purchased_at=purchased_at_datetime,
            )

            self._update_brevo_contact_after_purchase(
                email=email,
                funnel_type=funnel_type,
                purchased_at=purchased_at_datetime,
            )

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

    def _update_brevo_contact_after_purchase(
        self,
        email: str,
        funnel_type: str,
        purchased_at: datetime,
    ) -> None:
        """Updates Brevo contact attributes to reflect certificate purchase.

        Side Effects:
            - Updates contact attributes in Brevo (unless dry-run mode).
            - Does not modify list membership, only attributes.

        Args:
            email: Contact email address in Brevo.
            funnel_type: Funnel type for attribute tracking.
            purchased_at: Purchase timestamp for analytics.
        """
        attributes = {
            "FUNNEL_TYPE": funnel_type,
            "CERTIFICATE_PURCHASED": 1,
            "CERTIFICATE_PURCHASED_AT": purchased_at.isoformat(),
        }

        contact = BrevoContact(
            email=email,
            list_ids=[],
            attributes=attributes,
            update_enabled=True,
        )

        self.logger.info(
            "Updating Brevo contact after purchase (email=%s, funnel_type=%s)",
            email,
            funnel_type,
        )

        self.brevo_client.create_or_update_contact(contact)
