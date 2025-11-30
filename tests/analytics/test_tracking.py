from datetime import datetime

import mysql.connector
import pytest

from analytics import tracking


class DummyCursor:
    def __init__(self):
        self.executed_queries = []
        self.fetchone_result: tuple | None = None
        self.close_calls = 0
        self.lastrowid = 42

    def execute(self, query, params=None):
        self.executed_queries.append((query, params))

    def fetchone(self):
        return self.fetchone_result

    def close(self):
        self.close_calls += 1


class DummyConnection:
    def __init__(self, cursor):
        self._cursor = cursor
        self.commits = 0
        self.rollbacks = 0

    def cursor(self):
        return self._cursor

    def commit(self):
        self.commits += 1

    def rollback(self):
        self.rollbacks += 1


def test_funnel_entry_exists_returns_false_when_no_row():
    cursor = DummyCursor()
    cursor.fetchone_result = None
    connection = DummyConnection(cursor)

    result = tracking.funnel_entry_exists(
        connection=connection,  # type: ignore[arg-type]
        email="user@example.com",
        funnel_type="language",
        test_id=None,
    )

    assert result is False
    assert len(cursor.executed_queries) == 1
    query, params = cursor.executed_queries[0]
    assert "FROM funnel_entries" in query
    assert params == ("user@example.com", "language")


def test_funnel_entry_exists_returns_true_when_row_found_with_test_id():
    cursor = DummyCursor()
    cursor.fetchone_result = (1,)
    connection = DummyConnection(cursor)

    result = tracking.funnel_entry_exists(
        connection=connection,  # type: ignore[arg-type]
        email="user@example.com",
        funnel_type="language",
        test_id=42,
    )

    assert result is True
    assert len(cursor.executed_queries) == 1
    query, params = cursor.executed_queries[0]
    assert "AND test_id = %s" in query
    assert params == ("user@example.com", "language", 42)


def test_create_funnel_entry_inserts_and_returns_id():
    cursor = DummyCursor()
    cursor.lastrowid = 123
    connection = DummyConnection(cursor)

    result = tracking.create_funnel_entry(
        connection=connection,  # type: ignore[arg-type]
        email="user@example.com",
        funnel_type="language",
        user_id=10,
        test_id=42,
    )

    # Should return the new row ID
    assert result == 123
    # Should not call commit or rollback (transaction control is caller's responsibility)
    assert connection.commits == 0
    assert connection.rollbacks == 0
    assert cursor.close_calls == 1
    assert len(cursor.executed_queries) == 1
    query, params = cursor.executed_queries[0]
    assert "INSERT INTO funnel_entries" in query
    assert params[0] == "user@example.com"
    assert params[1] == "language"
    assert params[2] == 10
    assert params[3] == 42


def test_create_funnel_entry_handles_duplicate_gracefully():
    """Test that create_funnel_entry handles IntegrityError for duplicate entries."""
    cursor = DummyCursor()

    def execute_raises_integrity_error(query, params=None):
        raise mysql.connector.IntegrityError("Duplicate entry")

    cursor.execute = execute_raises_integrity_error
    connection = DummyConnection(cursor)

    # Should not raise an exception, should return None
    result = tracking.create_funnel_entry(
        connection=connection,  # type: ignore[arg-type]
        email="user@example.com",
        funnel_type="language",
        user_id=10,
        test_id=42,
    )

    # Should return None to indicate no new row was created
    assert result is None
    # Should not call commit or rollback (transaction control is caller's responsibility)
    assert connection.rollbacks == 0
    assert connection.commits == 0
    assert cursor.close_calls == 1


def test_create_funnel_entry_propagates_other_errors():
    """Test that create_funnel_entry propagates non-IntegrityError exceptions."""
    cursor = DummyCursor()

    def execute_raises_other_error(query, params=None):
        raise mysql.connector.Error("Connection lost")

    cursor.execute = execute_raises_other_error
    connection = DummyConnection(cursor)

    # Should raise the exception
    with pytest.raises(mysql.connector.Error):
        tracking.create_funnel_entry(
            connection=connection,  # type: ignore[arg-type]
            email="user@example.com",
            funnel_type="language",
            user_id=10,
            test_id=42,
        )

    # Should not commit or rollback (transaction control is caller's responsibility)
    # Cursor should still be closed in finally block
    assert connection.commits == 0
    assert connection.rollbacks == 0
    assert cursor.close_calls == 1


def test_mark_certificate_purchased_without_test_id_updates():
    cursor = DummyCursor()
    connection = DummyConnection(cursor)
    purchased_at = datetime(2025, 1, 1, 12, 0, 0)

    tracking.mark_certificate_purchased(
        connection=connection,  # type: ignore[arg-type]
        email="user@example.com",
        funnel_type="language",
        test_id=None,
        purchased_at=purchased_at,
    )

    # Should not call commit (transaction control is caller's responsibility)
    assert connection.commits == 0
    assert cursor.close_calls == 1
    assert len(cursor.executed_queries) == 1
    query, params = cursor.executed_queries[0]
    assert "UPDATE funnel_entries" in query
    assert "WHERE email = %s" in query
    assert "AND funnel_type = %s" in query
    assert "AND test_id = %s" not in query
    assert params == (purchased_at, "user@example.com", "language")


def test_mark_certificate_purchased_with_test_id_updates():
    cursor = DummyCursor()
    connection = DummyConnection(cursor)
    purchased_at = datetime(2025, 1, 1, 12, 0, 0)

    tracking.mark_certificate_purchased(
        connection=connection,  # type: ignore[arg-type]
        email="user@example.com",
        funnel_type="language",
        test_id=42,
        purchased_at=purchased_at,
    )

    # Should not call commit (transaction control is caller's responsibility)
    assert connection.commits == 0
    assert cursor.close_calls == 1
    assert len(cursor.executed_queries) == 1
    query, params = cursor.executed_queries[0]
    assert "UPDATE funnel_entries" in query
    assert "AND test_id = %s" in query
    assert params == (purchased_at, "user@example.com", "language", 42)
