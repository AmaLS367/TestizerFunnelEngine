"""Tests for brevo.outbox module."""

from brevo.outbox import (
    BrevoSyncJob,
    enqueue_brevo_sync_job,
    fetch_pending_jobs,
    mark_job_error,
    mark_job_success,
)


class DummyCursor:
    def __init__(self):
        self.executed_queries = []
        self.fetchall_result = []
        self.lastrowid = 42

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


def test_enqueue_brevo_sync_job_inserts_and_returns_id():
    """Test that enqueue_brevo_sync_job inserts a new job and returns the ID."""
    cursor = DummyCursor()
    cursor.lastrowid = 123
    connection = DummyConnection(cursor)

    job_id = enqueue_brevo_sync_job(
        connection=connection,  # type: ignore[arg-type]
        funnel_entry_id=10,
        operation_type="upsert_contact",
        payload='{"email": "user@example.com"}',
    )

    assert job_id == 123
    assert len(cursor.executed_queries) == 1
    query, params = cursor.executed_queries[0]
    assert "INSERT INTO brevo_sync_outbox" in query
    assert params[0] == 10
    assert params[1] == "upsert_contact"
    assert params[2] == '{"email": "user@example.com"}'


def test_fetch_pending_jobs_returns_list_of_jobs():
    """Test that fetch_pending_jobs returns a list of BrevoSyncJob instances."""
    cursor = DummyCursor()
    cursor.fetchall_result = [
        (1, 10, "upsert_contact", '{"email": "user@example.com"}', "pending", 0),
        (2, 11, "update_after_purchase", '{"email": "user2@example.com"}', "pending", 1),
    ]
    connection = DummyConnection(cursor)

    jobs = fetch_pending_jobs(connection=connection, limit=100)  # type: ignore[arg-type]

    assert len(jobs) == 2
    assert isinstance(jobs[0], BrevoSyncJob)
    assert jobs[0].id == 1
    assert jobs[0].funnel_entry_id == 10
    assert jobs[0].operation_type == "upsert_contact"
    assert jobs[0].payload == '{"email": "user@example.com"}'
    assert jobs[0].status == "pending"
    assert jobs[0].retry_count == 0

    assert isinstance(jobs[1], BrevoSyncJob)
    assert jobs[1].id == 2
    assert jobs[1].funnel_entry_id == 11
    assert jobs[1].operation_type == "update_after_purchase"
    assert jobs[1].retry_count == 1

    assert len(cursor.executed_queries) == 1
    query, params = cursor.executed_queries[0]
    assert "SELECT" in query
    assert "WHERE status = 'pending'" in query
    assert "ORDER BY id" in query
    assert "LIMIT" in query
    assert params[0] == 100


def test_fetch_pending_jobs_respects_limit():
    """Test that fetch_pending_jobs respects the limit parameter."""
    cursor = DummyCursor()
    cursor.fetchall_result = []
    connection = DummyConnection(cursor)

    jobs = fetch_pending_jobs(connection=connection, limit=50)  # type: ignore[arg-type]

    assert len(jobs) == 0
    query, params = cursor.executed_queries[0]
    assert params[0] == 50


def test_mark_job_success_updates_status():
    """Test that mark_job_success updates the job status to 'success'."""
    cursor = DummyCursor()
    connection = DummyConnection(cursor)

    mark_job_success(connection=connection, job_id=42)  # type: ignore[arg-type]

    assert len(cursor.executed_queries) == 1
    query, params = cursor.executed_queries[0]
    assert "UPDATE brevo_sync_outbox" in query
    assert "SET status = 'success'" in query
    assert "last_error = NULL" in query
    assert "WHERE id = %s" in query
    assert params[0] == 42


def test_mark_job_error_updates_status_and_increments_retry():
    """Test that mark_job_error updates status, sets error message, and increments retry_count."""
    cursor = DummyCursor()
    connection = DummyConnection(cursor)

    mark_job_error(
        connection=connection,  # type: ignore[arg-type]
        job_id=42,
        error_message="API request failed",
    )

    assert len(cursor.executed_queries) == 1
    query, params = cursor.executed_queries[0]
    assert "UPDATE brevo_sync_outbox" in query
    assert "SET status = 'error'" in query
    assert "last_error = %s" in query
    assert "retry_count = retry_count + 1" in query
    assert "WHERE id = %s" in query
    assert params[0] == "API request failed"
    assert params[1] == 42

