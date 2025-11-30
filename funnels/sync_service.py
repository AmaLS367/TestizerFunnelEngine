import json
import logging
from typing import List, Tuple

import mysql.connector
from mysql.connector import MySQLConnection

from analytics.tracking import create_funnel_entry
from brevo.api_client import BrevoApiClient
from brevo.outbox import enqueue_brevo_sync_job
from funnels.models import FunnelCandidate, FunnelType
from db.selectors import (
    get_language_test_candidates,
    get_non_language_test_candidates,
)


class FunnelSyncService:
    """Orchestrates synchronization of test candidates into email funnels.

    This service ensures idempotent processing: the same user will never be
    added to the same funnel twice, preventing duplicate Brevo API calls and
    maintaining data integrity. Idempotency is enforced at the database level
    by create_funnel_entry using a unique constraint on (email, funnel_type, test_id).
    """

    def __init__(
        self,
        connection: MySQLConnection,
        brevo_client: BrevoApiClient,
        language_list_id: int,
        non_language_list_id: int,
        dry_run: bool = False,
    ) -> None:
        """Initializes the funnel synchronization service.

        Args:
            connection: Active MySQL database connection for reading candidates
                and writing funnel entries.
            brevo_client: Brevo API client for contact management. Must be
                configured with appropriate API key and dry-run settings.
            language_list_id: Brevo list ID for language test funnel contacts.
                If <= 0, language funnel processing is skipped.
            non_language_list_id: Brevo list ID for non-language test funnel
                contacts. If <= 0, non-language funnel processing is skipped.
            dry_run: If True, no database writes or Brevo API calls are performed.
                Only read operations and logging occur.
        """
        self.connection = connection
        self.brevo_client = brevo_client
        self.language_list_id = language_list_id
        self.non_language_list_id = non_language_list_id
        self.dry_run = dry_run
        self.logger = logging.getLogger("funnels.sync_service")

    def sync(self, max_rows_per_type: int = 100) -> None:
        """Synchronizes both language and non-language funnels in a single batch.

        Fetches candidates from database selectors and processes them through
        the funnel pipeline: idempotency check → Brevo contact creation →
        funnel entry recording.

        The batch size (max_rows_per_type) is used to manage memory footprint
        during high-load periods. Processing stops at the limit even if more
        candidates exist, requiring subsequent runs to process remaining users.

        Side Effects:
            - Creates or updates contacts in Brevo (unless dry-run mode).
            - Inserts records into funnel_entries table.
            - Logs all operations for audit trail.

        Args:
            max_rows_per_type: Maximum candidates to process per funnel type.
                Lower values reduce memory usage but require more frequent runs.
        """
        self.logger.info("Starting funnel synchronization")

        language_rows = get_language_test_candidates(
            self.connection,
            limit=max_rows_per_type,
        )
        non_language_rows = get_non_language_test_candidates(
            self.connection,
            limit=max_rows_per_type,
        )

        self.logger.info(
            "Fetched %s language rows and %s non language rows",
            len(language_rows),
            len(non_language_rows),
        )

        self._sync_language_funnel(language_rows)
        self._sync_non_language_funnel(non_language_rows)

        self.logger.info("Funnel synchronization finished")

    def _sync_language_funnel(self, rows: List[Tuple[int, str]]) -> None:
        if self.language_list_id <= 0:
            self.logger.info(
                "Language list id is not configured, skipping language funnel",
            )
            return

        for row in rows:
            candidate = self._map_placeholder_row_to_candidate(
                row=row,
                funnel_type=FunnelType.LANGUAGE,
            )
            self._process_candidate(candidate, self.language_list_id)

    def _sync_non_language_funnel(self, rows: List[Tuple[int, str]]) -> None:
        if self.non_language_list_id <= 0:
            self.logger.info(
                "Non language list id is not configured, skipping non language funnel",
            )
            return

        for row in rows:
            candidate = self._map_placeholder_row_to_candidate(
                row=row,
                funnel_type=FunnelType.NON_LANGUAGE,
            )
            self._process_candidate(candidate, self.non_language_list_id)

    def _map_placeholder_row_to_candidate(
        self,
        row: Tuple[int, str],
        funnel_type: str,
    ) -> FunnelCandidate:
        """Maps database row tuple to FunnelCandidate domain model.

        Currently extracts user_id and email from selector results. Other fields
        (test_id, test_completed_at) are set to None as they're not yet available
        in the current selector implementation.

        Args:
            row: Tuple from database selector: (user_id, email).
            funnel_type: Either 'language' or 'non_language'.

        Returns:
            FunnelCandidate object ready for processing pipeline.
        """
        dummy_value, email = row

        candidate = FunnelCandidate(
            email=str(email),
            funnel_type=funnel_type,
            user_id=None,
            test_id=None,
            test_completed_at=None,
        )

        return candidate

    def _process_candidate(
        self,
        candidate: FunnelCandidate,
        list_id: int,
    ) -> None:
        """Processes a single candidate through the funnel pipeline.

        Idempotency is enforced at the database level by create_funnel_entry, which
        handles duplicate entries gracefully via the unique constraint on
        (email, funnel_type, test_id). If a duplicate entry already exists,
        create_funnel_entry returns None and no outbox job is enqueued.

        Side Effects:
            - In dry-run mode: only logs what would be done without making changes.
            - In production mode: creates funnel entry and enqueues Brevo sync job
              in a single atomic transaction.

        Args:
            candidate: User candidate extracted from test results.
            list_id: Brevo list ID where contact should be added.
        """
        if self.dry_run:
            self.logger.info(
                "Dry run: would create Brevo contact for (email=%s, funnel_type=%s, list_id=%s)",
                candidate.email,
                candidate.funnel_type,
                list_id,
            )
            self.logger.info(
                "Dry run: would create funnel entry for (email=%s, funnel_type=%s, test_id=%s)",
                candidate.email,
                candidate.funnel_type,
                candidate.test_id,
            )
            return

        try:
            self.connection.start_transaction()

            self.logger.info(
                "Creating funnel entry (email=%s, funnel_type=%s)",
                candidate.email,
                candidate.funnel_type,
            )

            funnel_entry_id = create_funnel_entry(
                connection=self.connection,
                email=candidate.email,
                funnel_type=candidate.funnel_type,
                user_id=candidate.user_id,
                test_id=candidate.test_id,
            )

            if funnel_entry_id is None:
                # Duplicate entry - commit and return without enqueuing outbox job
                self.connection.commit()
                self.logger.info(
                    "Duplicate funnel entry detected, skipping (email=%s, funnel_type=%s, test_id=%s)",
                    candidate.email,
                    candidate.funnel_type,
                    candidate.test_id,
                )
                return

            # Build JSON payload for Brevo sync job
            payload_data = {
                "email": candidate.email,
                "funnel_type": candidate.funnel_type,
                "user_id": candidate.user_id,
                "test_id": candidate.test_id,
                "list_ids": [list_id],
                "attributes": {
                    "FUNNEL_TYPE": candidate.funnel_type,
                },
            }
            payload_json = json.dumps(payload_data)

            self.logger.info(
                "Enqueuing Brevo sync job (funnel_entry_id=%s, operation_type=upsert_contact)",
                funnel_entry_id,
            )

            enqueue_brevo_sync_job(
                connection=self.connection,
                funnel_entry_id=funnel_entry_id,
                operation_type="upsert_contact",
                payload=payload_json,
            )

            self.connection.commit()

            self.logger.info(
                "Successfully processed candidate (email=%s, funnel_entry_id=%s)",
                candidate.email,
                funnel_entry_id,
            )

        except mysql.connector.Error as e:
            self.connection.rollback()
            self.logger.error(
                "Failed to process candidate (email=%s, funnel_type=%s): %s",
                candidate.email,
                candidate.funnel_type,
                str(e),
            )
            raise
