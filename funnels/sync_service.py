import logging
from typing import List, Tuple, Optional

from mysql.connector import MySQLConnection

from analytics.tracking import funnel_entry_exists, create_funnel_entry
from brevo.api_client import BrevoApiClient
from brevo.models import BrevoContact
from funnels.models import FunnelCandidate, FunnelType
from db.selectors import (
    get_language_test_candidates,
    get_non_language_test_candidates,
)


class FunnelSyncService:
    def __init__(
        self,
        connection: MySQLConnection,
        brevo_client: BrevoApiClient,
        language_list_id: int,
        non_language_list_id: int,
    ) -> None:
        self.connection = connection
        self.brevo_client = brevo_client
        self.language_list_id = language_list_id
        self.non_language_list_id = non_language_list_id
        self.logger = logging.getLogger("funnels.sync_service")

    def sync(self, max_rows_per_type: int = 100) -> None:
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

    def _sync_language_funnel(self, rows: List[Tuple]) -> None:
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

    def _sync_non_language_funnel(self, rows: List[Tuple]) -> None:
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
        row: Tuple,
        funnel_type: str,
    ) -> FunnelCandidate:
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
        if funnel_entry_exists(
            connection=self.connection,
            email=candidate.email,
            funnel_type=candidate.funnel_type,
            test_id=candidate.test_id,
        ):
            self.logger.info(
                "Candidate already in funnel, skipping (email=%s, funnel_type=%s)",
                candidate.email,
                candidate.funnel_type,
            )
            return

        brevo_contact = BrevoContact(
            email=candidate.email,
            list_ids=[list_id],
            attributes={
                "FUNNEL_TYPE": candidate.funnel_type,
            },
        )

        self.logger.info(
            "Sending candidate to Brevo (email=%s, funnel_type=%s, list_id=%s)",
            candidate.email,
            candidate.funnel_type,
            list_id,
        )

        self.brevo_client.create_or_update_contact(brevo_contact)

        self.logger.info(
            "Creating funnel entry (email=%s, funnel_type=%s)",
            candidate.email,
            candidate.funnel_type,
        )

        create_funnel_entry(
            connection=self.connection,
            email=candidate.email,
            funnel_type=candidate.funnel_type,
            user_id=candidate.user_id,
            test_id=candidate.test_id,
        )

