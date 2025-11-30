"""Integration tests for funnel sync and outbox processing using real MySQL database.

These tests verify the full flow from candidate selection through funnel entry
creation to outbox job processing.
"""

import json
from datetime import datetime, timedelta
from typing import Any, Dict, List

import pytest
from mysql.connector import MySQLConnection

from brevo.api_client import BrevoContact
from brevo.outbox import fetch_pending_jobs
from brevo.sync_worker import BrevoSyncWorker
from funnels.sync_service import FunnelSyncService


class FakeBrevoApiClient:
    """Fake Brevo API client that records calls without making network requests.

    Implements the same public interface as BrevoApiClient for testing purposes.
    """

    def __init__(self) -> None:
        """Initializes the fake client with empty call history."""
        self.calls: List[BrevoContact] = []

    def create_or_update_contact(self, contact: BrevoContact) -> Dict[str, Any]:
        """Records the contact call without making network request.

        Args:
            contact: BrevoContact object to record.

        Returns:
            Fake response dictionary.
        """
        self.calls.append(contact)
        return {"id": len(self.calls), "dry_run": True}


@pytest.mark.integration
def test_funnel_sync_creates_entry_and_outbox_job(
    mysql_test_connection: MySQLConnection,
) -> None:
    """Tests that funnel sync creates funnel entry and outbox job.

    Verifies the full flow:
    1. Insert candidate data
    2. Run FunnelSyncService.sync()
    3. Run BrevoSyncWorker.run_once()
    4. Assert funnel entry and outbox job are created correctly
    """
    cursor = mysql_test_connection.cursor()

    # Insert test data for a valid candidate
    cursor.execute("INSERT INTO simpletest_lang (Id) VALUES (1)")
    cursor.execute("INSERT INTO simpletest_test (Id, LangId) VALUES (1, 1)")

    now = datetime.now()
    recent_date = now - timedelta(days=10)

    cursor.execute(
        "INSERT INTO simpletest_users (Id, Email, TestId, Datep, Status) VALUES (%s, %s, %s, %s, %s)",
        (1, "test@example.com", 1, recent_date, 1),
    )

    mysql_test_connection.commit()
    cursor.close()

    # Create fake Brevo client
    fake_client = FakeBrevoApiClient()

    # Run funnel sync
    funnel_sync_service = FunnelSyncService(
        connection=mysql_test_connection,
        brevo_client=fake_client,  # type: ignore[arg-type]
        language_list_id=100,
        non_language_list_id=0,
        dry_run=False,
    )

    funnel_sync_service.sync(max_rows_per_type=10)

    # Verify funnel entry was created
    cursor = mysql_test_connection.cursor()
    cursor.execute(
        "SELECT id, email, funnel_type, user_id, test_id FROM funnel_entries WHERE email = %s",
        ("test@example.com",),
    )
    funnel_entry = cursor.fetchone()
    cursor.close()

    assert funnel_entry is not None, "Funnel entry should be created"
    assert funnel_entry[1] == "test@example.com"
    assert funnel_entry[2] == "language"
    # Note: user_id and test_id may be None in current implementation

    funnel_entry_id = funnel_entry[0]

    # Verify outbox job was created
    jobs = fetch_pending_jobs(mysql_test_connection, limit=100)
    assert len(jobs) == 1, "Should have exactly one pending job"

    job = jobs[0]
    assert job.funnel_entry_id == funnel_entry_id
    assert job.operation_type == "upsert_contact"
    assert job.status == "pending"

    # Verify payload contains expected data
    payload_data = json.loads(job.payload)
    assert payload_data["email"] == "test@example.com"
    assert payload_data["funnel_type"] == "language"
    # Note: user_id and test_id may be None in current implementation
    assert "list_ids" in payload_data
    assert payload_data["list_ids"] == [100]

    # Run Brevo sync worker
    brevo_sync_worker = BrevoSyncWorker(
        connection=mysql_test_connection,
        brevo_client=fake_client,  # type: ignore[arg-type]
    )

    brevo_sync_worker.run_once(limit=100)

    # Verify job was processed (marked as success)
    jobs_after = fetch_pending_jobs(mysql_test_connection, limit=100)
    assert len(jobs_after) == 0, "Job should be marked as success and no longer pending"

    # Verify fake client was called
    assert len(fake_client.calls) == 1, "Fake client should have been called once"
    assert fake_client.calls[0].email == "test@example.com"
    assert fake_client.calls[0].list_ids == [100]


@pytest.mark.integration
def test_funnel_sync_skips_existing_entries(
    mysql_test_connection: MySQLConnection,
) -> None:
    """Tests that funnel sync skips users already in funnel_entries."""
    cursor = mysql_test_connection.cursor()

    # Insert test data
    cursor.execute("INSERT INTO simpletest_lang (Id) VALUES (1)")
    cursor.execute("INSERT INTO simpletest_test (Id, LangId) VALUES (1, 1)")

    now = datetime.now()
    recent_date = now - timedelta(days=10)

    cursor.execute(
        "INSERT INTO simpletest_users (Id, Email, TestId, Datep, Status) VALUES (%s, %s, %s, %s, %s)",
        (1, "existing@example.com", 1, recent_date, 1),
    )

    # Pre-create funnel entry
    cursor.execute(
        "INSERT INTO funnel_entries (email, funnel_type, user_id, test_id) VALUES (%s, %s, %s, %s)",
        ("existing@example.com", "language", 1, 1),
    )

    mysql_test_connection.commit()
    cursor.close()

    # Create fake Brevo client
    fake_client = FakeBrevoApiClient()

    # Run funnel sync
    funnel_sync_service = FunnelSyncService(
        connection=mysql_test_connection,
        brevo_client=fake_client,  # type: ignore[arg-type]
        language_list_id=100,
        non_language_list_id=0,
        dry_run=False,
    )

    funnel_sync_service.sync(max_rows_per_type=10)

    # Verify no new outbox job was created
    jobs = fetch_pending_jobs(mysql_test_connection, limit=100)
    assert len(jobs) == 0, "Should not create job for existing entry"

    # Verify no new funnel entry was created (count should still be 1)
    cursor = mysql_test_connection.cursor()
    cursor.execute("SELECT COUNT(*) FROM funnel_entries WHERE email = %s", ("existing@example.com",))
    count = cursor.fetchone()[0]
    cursor.close()

    assert count == 1, "Should not create duplicate funnel entry"

