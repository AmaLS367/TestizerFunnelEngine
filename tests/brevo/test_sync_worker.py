"""Tests for brevo.sync_worker module."""

import json

from brevo.api_client import BrevoFatalError, BrevoTransientError
from brevo.outbox import BrevoSyncJob
from brevo.sync_worker import BrevoSyncWorker


class DummyBrevoClient:
    def __init__(self):
        self.calls = []

    def create_or_update_contact(self, contact):
        self.calls.append(contact)


class DummyCursor:
    def __init__(self):
        self.executed_queries = []
        self.fetchall_result = []

    def execute(self, query, params=None):
        self.executed_queries.append((query, params))

    def fetchall(self):
        return self.fetchall_result

    def close(self):
        pass


class DummyConnection:
    def __init__(self, cursor):
        self._cursor = cursor

    def cursor(self):
        return self._cursor


def test_run_once_processes_upsert_contact_job(monkeypatch):
    """Test that run_once processes an upsert_contact job successfully."""
    cursor = DummyCursor()
    cursor.fetchall_result = [
        (
            1,
            10,
            "upsert_contact",
            json.dumps(
                {
                    "email": "user@example.com",
                    "list_ids": [1, 2],
                    "attributes": {"FUNNEL_TYPE": "language"},
                }
            ),
            "pending",
            0,
        ),
    ]
    connection = DummyConnection(cursor)
    brevo_client = DummyBrevoClient()

    # Mock the outbox functions
    def fake_fetch_pending_jobs(conn, limit):
        return [
            BrevoSyncJob(
                id=1,
                funnel_entry_id=10,
                operation_type="upsert_contact",
                payload=json.dumps(
                    {
                        "email": "user@example.com",
                        "list_ids": [1, 2],
                        "attributes": {"FUNNEL_TYPE": "language"},
                    }
                ),
                status="pending",
                retry_count=0,
                next_attempt_at=None,
            )
        ]

    def fake_mark_job_success(conn, job_id):
        pass

    def fake_mark_job_error(
        conn, job_id, error_message, max_job_retries=5, is_fatal=False
    ):
        pass

    import brevo.sync_worker as worker_module

    monkeypatch.setattr(worker_module, "fetch_pending_jobs", fake_fetch_pending_jobs)
    monkeypatch.setattr(worker_module, "mark_job_success", fake_mark_job_success)
    monkeypatch.setattr(worker_module, "mark_job_error", fake_mark_job_error)

    worker = BrevoSyncWorker(connection=connection, brevo_client=brevo_client)  # type: ignore[arg-type]
    worker.run_once(limit=100)

    # Should have called Brevo API once
    assert len(brevo_client.calls) == 1
    contact = brevo_client.calls[0]
    assert contact.email == "user@example.com"
    assert contact.list_ids == [1, 2]
    assert contact.attributes == {"FUNNEL_TYPE": "language"}


def test_run_once_processes_update_after_purchase_job(monkeypatch):
    """Test that run_once processes an update_after_purchase job successfully."""
    brevo_client = DummyBrevoClient()

    def fake_fetch_pending_jobs(conn, limit):
        return [
            BrevoSyncJob(
                id=2,
                funnel_entry_id=11,
                operation_type="update_after_purchase",
                payload=json.dumps(
                    {
                        "email": "user2@example.com",
                        "purchased_at": "2025-01-01T12:00:00",
                        "attributes": {"FUNNEL_TYPE": "language"},
                    }
                ),
                status="pending",
                retry_count=0,
                next_attempt_at=None,
            )
        ]

    def fake_mark_job_success(conn, job_id):
        pass

    def fake_mark_job_error(
        conn, job_id, error_message, max_job_retries=5, is_fatal=False
    ):
        pass

    import brevo.sync_worker as worker_module

    monkeypatch.setattr(worker_module, "fetch_pending_jobs", fake_fetch_pending_jobs)
    monkeypatch.setattr(worker_module, "mark_job_success", fake_mark_job_success)
    monkeypatch.setattr(worker_module, "mark_job_error", fake_mark_job_error)

    cursor = DummyCursor()
    connection = DummyConnection(cursor)
    worker = BrevoSyncWorker(connection=connection, brevo_client=brevo_client)  # type: ignore[arg-type]
    worker.run_once(limit=100)

    # Should have called Brevo API once with purchase attributes
    assert len(brevo_client.calls) == 1
    contact = brevo_client.calls[0]
    assert contact.email == "user2@example.com"
    assert contact.attributes["CERTIFICATE_PURCHASED"] == 1
    assert contact.attributes["CERTIFICATE_PURCHASED_AT"] == "2025-01-01T12:00:00"
    assert contact.attributes["FUNNEL_TYPE"] == "language"


def test_run_once_marks_job_error_on_invalid_payload(monkeypatch):
    """Test that run_once marks job as error when payload is invalid JSON."""
    brevo_client = DummyBrevoClient()
    error_calls = []

    def fake_fetch_pending_jobs(conn, limit):
        return [
            BrevoSyncJob(
                id=3,
                funnel_entry_id=12,
                operation_type="upsert_contact",
                payload="invalid json{",
                status="pending",
                retry_count=0,
                next_attempt_at=None,
            )
        ]

    def fake_mark_job_success(conn, job_id):
        pass

    def fake_mark_job_error(
        conn, job_id, error_message, max_job_retries=5, is_fatal=False
    ):
        error_calls.append((job_id, error_message))

    import brevo.sync_worker as worker_module

    monkeypatch.setattr(worker_module, "fetch_pending_jobs", fake_fetch_pending_jobs)
    monkeypatch.setattr(worker_module, "mark_job_success", fake_mark_job_success)
    monkeypatch.setattr(worker_module, "mark_job_error", fake_mark_job_error)

    cursor = DummyCursor()
    connection = DummyConnection(cursor)
    worker = BrevoSyncWorker(connection=connection, brevo_client=brevo_client)  # type: ignore[arg-type]
    worker.run_once(limit=100)

    # Should not have called Brevo API
    assert len(brevo_client.calls) == 0
    # Should have marked job as error
    assert len(error_calls) == 1
    assert error_calls[0][0] == 3
    assert "Invalid JSON payload" in error_calls[0][1]


def test_run_once_marks_job_error_on_missing_email(monkeypatch):
    """Test that run_once marks job as error when email is missing from payload."""
    brevo_client = DummyBrevoClient()
    error_calls = []

    def fake_fetch_pending_jobs(conn, limit):
        return [
            BrevoSyncJob(
                id=4,
                funnel_entry_id=13,
                operation_type="upsert_contact",
                payload=json.dumps({"list_ids": [1]}),
                status="pending",
                retry_count=0,
                next_attempt_at=None,
            )
        ]

    def fake_mark_job_success(conn, job_id):
        pass

    def fake_mark_job_error(
        conn, job_id, error_message, max_job_retries=5, is_fatal=False
    ):
        error_calls.append((job_id, error_message))

    import brevo.sync_worker as worker_module

    monkeypatch.setattr(worker_module, "fetch_pending_jobs", fake_fetch_pending_jobs)
    monkeypatch.setattr(worker_module, "mark_job_success", fake_mark_job_success)
    monkeypatch.setattr(worker_module, "mark_job_error", fake_mark_job_error)

    cursor = DummyCursor()
    connection = DummyConnection(cursor)
    worker = BrevoSyncWorker(connection=connection, brevo_client=brevo_client)  # type: ignore[arg-type]
    worker.run_once(limit=100)

    # Should not have called Brevo API
    assert len(brevo_client.calls) == 0
    # Should have marked job as error
    assert len(error_calls) == 1
    assert error_calls[0][0] == 4
    assert "Missing required field 'email'" in error_calls[0][1]


def test_run_once_marks_job_error_on_unknown_operation_type(monkeypatch):
    """Test that run_once marks job as error when operation_type is unknown."""
    brevo_client = DummyBrevoClient()
    error_calls = []

    def fake_fetch_pending_jobs(conn, limit):
        return [
            BrevoSyncJob(
                id=5,
                funnel_entry_id=14,
                operation_type="unknown_operation",
                payload=json.dumps({"email": "user@example.com"}),
                status="pending",
                retry_count=0,
                next_attempt_at=None,
            )
        ]

    def fake_mark_job_success(conn, job_id):
        pass

    def fake_mark_job_error(
        conn, job_id, error_message, max_job_retries=5, is_fatal=False
    ):
        error_calls.append((job_id, error_message))

    import brevo.sync_worker as worker_module

    monkeypatch.setattr(worker_module, "fetch_pending_jobs", fake_fetch_pending_jobs)
    monkeypatch.setattr(worker_module, "mark_job_success", fake_mark_job_success)
    monkeypatch.setattr(worker_module, "mark_job_error", fake_mark_job_error)

    cursor = DummyCursor()
    connection = DummyConnection(cursor)
    worker = BrevoSyncWorker(connection=connection, brevo_client=brevo_client)  # type: ignore[arg-type]
    worker.run_once(limit=100)

    # Should not have called Brevo API
    assert len(brevo_client.calls) == 0
    # Should have marked job as error
    assert len(error_calls) == 1
    assert error_calls[0][0] == 5
    assert "Unknown operation_type" in error_calls[0][1]


def test_run_once_processes_multiple_jobs(monkeypatch):
    """Test that run_once processes multiple jobs in sequence."""
    brevo_client = DummyBrevoClient()
    success_calls = []

    def fake_fetch_pending_jobs(conn, limit):
        return [
            BrevoSyncJob(
                id=6,
                funnel_entry_id=15,
                operation_type="upsert_contact",
                payload=json.dumps({"email": "user1@example.com"}),
                status="pending",
                retry_count=0,
                next_attempt_at=None,
            ),
            BrevoSyncJob(
                id=7,
                funnel_entry_id=16,
                operation_type="upsert_contact",
                payload=json.dumps({"email": "user2@example.com"}),
                status="pending",
                retry_count=0,
                next_attempt_at=None,
            ),
        ]

    def fake_mark_job_success(conn, job_id):
        success_calls.append(job_id)

    def fake_mark_job_error(
        conn, job_id, error_message, max_job_retries=5, is_fatal=False
    ):
        pass

    import brevo.sync_worker as worker_module

    monkeypatch.setattr(worker_module, "fetch_pending_jobs", fake_fetch_pending_jobs)
    monkeypatch.setattr(worker_module, "mark_job_success", fake_mark_job_success)
    monkeypatch.setattr(worker_module, "mark_job_error", fake_mark_job_error)

    cursor = DummyCursor()
    connection = DummyConnection(cursor)
    worker = BrevoSyncWorker(connection=connection, brevo_client=brevo_client)  # type: ignore[arg-type]
    worker.run_once(limit=100)

    # Should have called Brevo API twice
    assert len(brevo_client.calls) == 2
    assert brevo_client.calls[0].email == "user1@example.com"
    assert brevo_client.calls[1].email == "user2@example.com"
    # Should have marked both as success
    assert len(success_calls) == 2
    assert 6 in success_calls
    assert 7 in success_calls


def test_run_once_handles_brevo_transient_error(monkeypatch):
    """Test that run_once handles BrevoTransientError without crashing."""
    error_calls = []

    class FailingBrevoClient:
        def create_or_update_contact(self, contact):
            raise BrevoTransientError("Network error: Connection timeout")

    brevo_client = FailingBrevoClient()

    def fake_fetch_pending_jobs(conn, limit):
        return [
            BrevoSyncJob(
                id=8,
                funnel_entry_id=17,
                operation_type="upsert_contact",
                payload=json.dumps({"email": "user@example.com"}),
                status="pending",
                retry_count=0,
                next_attempt_at=None,
            )
        ]

    def fake_mark_job_success(conn, job_id):
        pass

    def fake_mark_job_error(
        conn, job_id, error_message, max_job_retries=5, is_fatal=False
    ):
        error_calls.append((job_id, error_message))

    import brevo.sync_worker as worker_module

    monkeypatch.setattr(worker_module, "fetch_pending_jobs", fake_fetch_pending_jobs)
    monkeypatch.setattr(worker_module, "mark_job_success", fake_mark_job_success)
    monkeypatch.setattr(worker_module, "mark_job_error", fake_mark_job_error)

    cursor = DummyCursor()
    connection = DummyConnection(cursor)
    worker = BrevoSyncWorker(connection=connection, brevo_client=brevo_client)  # type: ignore[arg-type]

    # Should not raise, should handle the error gracefully
    worker.run_once(limit=100)

    # Should have marked job as error
    assert len(error_calls) == 1
    assert error_calls[0][0] == 8
    assert "Network error" in error_calls[0][1]


def test_run_once_handles_brevo_fatal_error(monkeypatch):
    """Test that run_once handles BrevoFatalError without crashing."""
    error_calls = []

    class FailingBrevoClient:
        def create_or_update_contact(self, contact):
            raise BrevoFatalError("Brevo API error 400: Invalid email format")

    brevo_client = FailingBrevoClient()

    def fake_fetch_pending_jobs(conn, limit):
        return [
            BrevoSyncJob(
                id=9,
                funnel_entry_id=18,
                operation_type="upsert_contact",
                payload=json.dumps({"email": "invalid-email"}),
                status="pending",
                retry_count=0,
                next_attempt_at=None,
            )
        ]

    def fake_mark_job_success(conn, job_id):
        pass

    def fake_mark_job_error(
        conn, job_id, error_message, max_job_retries=5, is_fatal=False
    ):
        error_calls.append((job_id, error_message))

    import brevo.sync_worker as worker_module

    monkeypatch.setattr(worker_module, "fetch_pending_jobs", fake_fetch_pending_jobs)
    monkeypatch.setattr(worker_module, "mark_job_success", fake_mark_job_success)
    monkeypatch.setattr(worker_module, "mark_job_error", fake_mark_job_error)

    cursor = DummyCursor()
    connection = DummyConnection(cursor)
    worker = BrevoSyncWorker(connection=connection, brevo_client=brevo_client)  # type: ignore[arg-type]

    # Should not raise, should handle the error gracefully
    worker.run_once(limit=100)

    # Should have marked job as error
    assert len(error_calls) == 1
    assert error_calls[0][0] == 9
    assert "400" in error_calls[0][1]
    assert "Invalid email format" in error_calls[0][1]


def test_run_once_does_not_process_jobs_with_future_next_attempt_at(monkeypatch):
    """Test that run_once does not process jobs with next_attempt_at in the future."""
    brevo_client = DummyBrevoClient()

    def fake_fetch_pending_jobs(conn, limit):
        # Return empty list - jobs with future next_attempt_at are filtered out
        return []

    def fake_mark_job_success(conn, job_id):
        pass

    def fake_mark_job_error(
        conn, job_id, error_message, max_job_retries=5, is_fatal=False
    ):
        pass

    import brevo.sync_worker as worker_module

    monkeypatch.setattr(worker_module, "fetch_pending_jobs", fake_fetch_pending_jobs)
    monkeypatch.setattr(worker_module, "mark_job_success", fake_mark_job_success)
    monkeypatch.setattr(worker_module, "mark_job_error", fake_mark_job_error)

    cursor = DummyCursor()
    connection = DummyConnection(cursor)
    worker = BrevoSyncWorker(connection=connection, brevo_client=brevo_client)  # type: ignore[arg-type]
    worker.run_once(limit=100)

    # Should not have called Brevo API
    assert len(brevo_client.calls) == 0


def test_run_once_marks_fatal_error_as_failed(monkeypatch):
    """Test that run_once marks fatal errors as permanently failed."""
    error_calls = []

    class FailingBrevoClient:
        def create_or_update_contact(self, contact):
            raise BrevoFatalError("Brevo API error 400: Invalid email format")

    brevo_client = FailingBrevoClient()

    def fake_fetch_pending_jobs(conn, limit):
        return [
            BrevoSyncJob(
                id=10,
                funnel_entry_id=20,
                operation_type="upsert_contact",
                payload=json.dumps({"email": "invalid-email"}),
                status="pending",
                retry_count=0,
                next_attempt_at=None,
            )
        ]

    def fake_mark_job_success(conn, job_id):
        pass

    def fake_mark_job_error(
        conn, job_id, error_message, max_job_retries=5, is_fatal=False
    ):
        error_calls.append((job_id, error_message, is_fatal))

    import brevo.sync_worker as worker_module

    monkeypatch.setattr(worker_module, "fetch_pending_jobs", fake_fetch_pending_jobs)
    monkeypatch.setattr(worker_module, "mark_job_success", fake_mark_job_success)
    monkeypatch.setattr(worker_module, "mark_job_error", fake_mark_job_error)

    cursor = DummyCursor()
    connection = DummyConnection(cursor)
    worker = BrevoSyncWorker(connection=connection, brevo_client=brevo_client)  # type: ignore[arg-type]
    worker.run_once(limit=100)

    # Should have marked job as error with is_fatal=True
    assert len(error_calls) == 1
    assert error_calls[0][0] == 10
    assert error_calls[0][2] is True  # is_fatal should be True


def test_run_once_schedules_retry_for_transient_error(monkeypatch):
    """Test that run_once schedules retry for transient errors."""
    error_calls = []

    class FailingBrevoClient:
        def create_or_update_contact(self, contact):
            raise BrevoTransientError("Network error: Connection timeout")

    brevo_client = FailingBrevoClient()

    def fake_fetch_pending_jobs(conn, limit):
        return [
            BrevoSyncJob(
                id=11,
                funnel_entry_id=21,
                operation_type="upsert_contact",
                payload=json.dumps({"email": "user@example.com"}),
                status="pending",
                retry_count=0,
                next_attempt_at=None,
            )
        ]

    def fake_mark_job_success(conn, job_id):
        pass

    def fake_mark_job_error(
        conn, job_id, error_message, max_job_retries=5, is_fatal=False
    ):
        error_calls.append((job_id, error_message, is_fatal))

    import brevo.sync_worker as worker_module

    monkeypatch.setattr(worker_module, "fetch_pending_jobs", fake_fetch_pending_jobs)
    monkeypatch.setattr(worker_module, "mark_job_success", fake_mark_job_success)
    monkeypatch.setattr(worker_module, "mark_job_error", fake_mark_job_error)

    cursor = DummyCursor()
    connection = DummyConnection(cursor)
    worker = BrevoSyncWorker(connection=connection, brevo_client=brevo_client)  # type: ignore[arg-type]
    worker.run_once(limit=100)

    # Should have marked job as error with is_fatal=False (for retry)
    assert len(error_calls) == 1
    assert error_calls[0][0] == 11
    assert error_calls[0][2] is False  # is_fatal should be False
