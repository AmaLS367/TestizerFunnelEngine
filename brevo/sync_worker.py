"""Worker for processing Brevo synchronization jobs from the outbox.

This module provides BrevoSyncWorker which fetches pending jobs from brevo_sync_outbox
and processes them by calling the Brevo API. Transaction boundaries are managed by the caller.
"""

import json
import logging
from typing import Any, Dict

from mysql.connector import MySQLConnection

from brevo.api_client import BrevoApiClient, BrevoFatalError, BrevoTransientError
from brevo.models import BrevoContact
from brevo.outbox import (
    BrevoSyncJob,
    fetch_pending_jobs,
    mark_job_error,
    mark_job_success,
)


class BrevoSyncWorker:
    """Processes Brevo synchronization jobs from the outbox table.

    Fetches pending jobs and processes them by calling the Brevo API. Each job
    is tried exactly once per run. Success and error states are recorded in
    the outbox table.
    """

    def __init__(
        self,
        connection: MySQLConnection,
        brevo_client: BrevoApiClient,
    ) -> None:
        """Initializes the Brevo sync worker.

        Args:
            connection: Active MySQL database connection for reading and updating
                jobs in brevo_sync_outbox.
            brevo_client: Brevo API client for making API calls.
        """
        self.connection = connection
        self.brevo_client = brevo_client
        self.logger = logging.getLogger("brevo.sync_worker")

    def run_once(self, limit: int = 100) -> None:
        """Fetch up to `limit` pending jobs from brevo_sync_outbox and process them once.

        For each job:
        - Parse job.payload (assumed to be a JSON string with all required fields).
        - For operation_type == "upsert_contact": call brevo_client.create_or_update_contact(...).
        - For operation_type == "update_after_purchase": call brevo_client.create_or_update_contact(...)
          with updated attributes.
        - On success, mark the job as success.
        - On exception, mark the job as error with the error message.

        Transaction boundaries will be managed by the caller.

        Args:
            limit: Maximum number of jobs to process in this run. Defaults to 100.
        """
        jobs = fetch_pending_jobs(self.connection, limit=limit)

        self.logger.info("Processing %s pending Brevo sync jobs", len(jobs))

        for job in jobs:
            try:
                self._process_job(job)
                mark_job_success(self.connection, job.id)
                self.logger.info(
                    "Successfully processed job %s (operation_type=%s)",
                    job.id,
                    job.operation_type,
                )
            except BrevoTransientError as e:
                error_message = str(e)
                mark_job_error(self.connection, job.id, error_message, is_fatal=False)
                self.logger.warning(
                    "Transient error processing job %s (operation_type=%s): %s. "
                    "Job will be retried.",
                    job.id,
                    job.operation_type,
                    error_message,
                )
            except BrevoFatalError as e:
                error_message = str(e)
                mark_job_error(self.connection, job.id, error_message, is_fatal=True)
                self.logger.error(
                    "Fatal error processing job %s (operation_type=%s): %s. "
                    "Job marked as permanently failed.",
                    job.id,
                    job.operation_type,
                    error_message,
                )
            except Exception as e:
                error_message = str(e)
                # Unknown exceptions are treated as transient for retry
                mark_job_error(self.connection, job.id, error_message, is_fatal=False)
                self.logger.error(
                    "Failed to process job %s (operation_type=%s): %s. "
                    "Job will be retried.",
                    job.id,
                    job.operation_type,
                    error_message,
                )

    def _process_job(self, job: BrevoSyncJob) -> None:
        """Processes a single Brevo sync job.

        Args:
            job: The Brevo sync job to process.

        Raises:
            ValueError: If operation_type is not recognized or payload is invalid.
            BrevoTransientError: If Brevo API call fails with transient error.
            BrevoFatalError: If Brevo API call fails with fatal error.
        """
        try:
            payload_data = json.loads(job.payload)
        except json.JSONDecodeError as e:
            raise ValueError(f"Invalid JSON payload for job {job.id}: {e}") from e

        if job.operation_type == "upsert_contact":
            self._process_upsert_contact(payload_data)
        elif job.operation_type == "update_after_purchase":
            self._process_update_after_purchase(payload_data)
        else:
            raise ValueError(
                f"Unknown operation_type '{job.operation_type}' for job {job.id}"
            )

    def _process_upsert_contact(self, payload_data: Dict[str, Any]) -> None:
        """Processes an upsert_contact operation.

        Args:
            payload_data: Parsed JSON payload containing contact data.

        Raises:
            ValueError: If required fields are missing.
            BrevoTransientError: If Brevo API call fails with transient error.
            BrevoFatalError: If Brevo API call fails with fatal error.
        """
        if "email" not in payload_data:
            raise ValueError("Missing required field 'email' in payload")

        contact = BrevoContact(
            email=payload_data["email"],
            list_ids=payload_data.get("list_ids", []),
            attributes=payload_data.get("attributes", {}),
            update_enabled=payload_data.get("update_enabled", True),
        )

        self.brevo_client.create_or_update_contact(contact)

    def _process_update_after_purchase(
        self,
        payload_data: Dict[str, Any],
    ) -> None:
        """Processes an update_after_purchase operation.

        For now, this reuses create_or_update_contact with updated attributes
        to reflect the purchase status.

        Args:
            payload_data: Parsed JSON payload containing contact data and purchase info.

        Raises:
            ValueError: If required fields are missing.
            BrevoTransientError: If Brevo API call fails with transient error.
            BrevoFatalError: If Brevo API call fails with fatal error.
        """
        if "email" not in payload_data:
            raise ValueError("Missing required field 'email' in payload")

        # Merge purchase-related attributes into the attributes dict
        attributes = payload_data.get("attributes", {}).copy()
        attributes.update(
            {
                "CERTIFICATE_PURCHASED": 1,
                "CERTIFICATE_PURCHASED_AT": payload_data.get("purchased_at", ""),
            }
        )

        contact = BrevoContact(
            email=payload_data["email"],
            list_ids=payload_data.get("list_ids", []),
            attributes=attributes,
            update_enabled=True,
        )

        self.brevo_client.create_or_update_contact(contact)
