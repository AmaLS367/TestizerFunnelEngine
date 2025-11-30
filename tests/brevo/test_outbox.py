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
        (1, 10, "upsert_contact", '{"email": "user@example.com"}', "pending", 0, None),
        (
            2,
            11,
            "update_after_purchase",
            '{"email": "user2@example.com"}',
            "pending",
            1,
            None,
        ),
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
    assert jobs[0].next_attempt_at is None

    assert isinstance(jobs[1], BrevoSyncJob)
    assert jobs[1].id == 2
    assert jobs[1].funnel_entry_id == 11
    assert jobs[1].operation_type == "update_after_purchase"
    assert jobs[1].retry_count == 1
    assert jobs[1].next_attempt_at is None

    assert len(cursor.executed_queries) == 1
    query, params = cursor.executed_queries[0]
    assert "SELECT" in query
    assert "WHERE status = 'pending'" in query
    assert "next_attempt_at" in query
    assert "(next_attempt_at IS NULL OR next_attempt_at <= NOW())" in query
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


def test_mark_job_error_schedules_retry_when_below_max():
    """Test that mark_job_error schedules retry when retry_count is below max."""
    cursor = DummyCursor()
    connection = DummyConnection(cursor)

    mark_job_error(
        connection=connection,  # type: ignore[arg-type]
        job_id=42,
        error_message="API request failed",
        max_job_retries=5,
        is_fatal=False,
    )

    assert len(cursor.executed_queries) == 1
    query, params = cursor.executed_queries[0]
    assert "UPDATE brevo_sync_outbox" in query
    assert "retry_count = retry_count + 1" in query
    assert "last_error = %s" in query
    assert "CASE" in query
    assert "WHEN retry_count + 1 <= %s THEN 'pending'" in query
    assert "ELSE 'failed'" in query
    assert "INTERVAL (retry_count + 1) * 5 MINUTE" in query
    assert params[0] == "API request failed"
    assert params[1] == 5  # max_job_retries
    assert params[2] == 5  # max_job_retries (used twice)
    assert params[3] == 42


def test_mark_job_error_marks_failed_when_fatal():
    """Test that mark_job_error marks job as failed immediately when is_fatal=True."""
    cursor = DummyCursor()
    connection = DummyConnection(cursor)

    mark_job_error(
        connection=connection,  # type: ignore[arg-type]
        job_id=42,
        error_message="Fatal API error",
        is_fatal=True,
    )

    assert len(cursor.executed_queries) == 1
    query, params = cursor.executed_queries[0]
    assert "UPDATE brevo_sync_outbox" in query
    assert "SET status = 'failed'" in query
    assert "retry_count = retry_count + 1" in query
    assert "next_attempt_at = NULL" in query
    assert params[0] == "Fatal API error"
    assert params[1] == 42


def test_fetch_pending_jobs_filters_by_next_attempt_at():
    """Test that fetch_pending_jobs only returns jobs with due next_attempt_at."""
    cursor = DummyCursor()
    cursor.fetchall_result = [
        (1, 10, "upsert_contact", '{"email": "user@example.com"}', "pending", 0, None),
    ]
    connection = DummyConnection(cursor)

    fetch_pending_jobs(connection=connection, limit=100)  # type: ignore[arg-type]

    assert len(cursor.executed_queries) == 1
    query, params = cursor.executed_queries[0]
    # Should filter by next_attempt_at
    assert "(next_attempt_at IS NULL OR next_attempt_at <= NOW())" in query


def test_mark_job_error_increments_retry_count():
    """Test that mark_job_error increments retry_count."""
    cursor = DummyCursor()
    connection = DummyConnection(cursor)

    mark_job_error(
        connection=connection,  # type: ignore[arg-type]
        job_id=42,
        error_message="Error",
        max_job_retries=5,
    )

    query, params = cursor.executed_queries[0]
    assert "retry_count = retry_count + 1" in query


def test_mark_job_error_sets_next_attempt_at_for_retry():
    """Test that mark_job_error sets next_attempt_at when scheduling retry."""
    cursor = DummyCursor()
    connection = DummyConnection(cursor)

    mark_job_error(
        connection=connection,  # type: ignore[arg-type]
        job_id=42,
        error_message="Error",
        max_job_retries=5,
        is_fatal=False,
    )

    query, params = cursor.executed_queries[0]
    assert "next_attempt_at = CASE" in query
    assert "INTERVAL (retry_count + 1) * 5 MINUTE" in query


def test_mark_job_error_clears_next_attempt_at_when_failed():
    """Test that mark_job_error clears next_attempt_at when job is marked as failed."""
    cursor = DummyCursor()
    connection = DummyConnection(cursor)

    mark_job_error(
        connection=connection,  # type: ignore[arg-type]
        job_id=42,
        error_message="Fatal error",
        is_fatal=True,
    )

    query, params = cursor.executed_queries[0]
    assert "next_attempt_at = NULL" in query
    assert "status = 'failed'" in query
