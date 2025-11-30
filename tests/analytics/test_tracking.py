from datetime import datetime

from analytics import tracking


class DummyCursor:
    def __init__(self):
        self.executed_queries = []
        self.fetchone_result: tuple | None = None

    def execute(self, query, params=None):
        self.executed_queries.append((query, params))

    def fetchone(self):
        return self.fetchone_result

    def close(self):
        pass


class DummyConnection:
    def __init__(self, cursor):
        self._cursor = cursor
        self.commits = 0

    def cursor(self):
        return self._cursor

    def commit(self):
        self.commits += 1


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


def test_create_funnel_entry_inserts_and_commits():
    cursor = DummyCursor()
    connection = DummyConnection(cursor)

    tracking.create_funnel_entry(
        connection=connection,  # type: ignore[arg-type]
        email="user@example.com",
        funnel_type="language",
        user_id=10,
        test_id=42,
    )

    assert connection.commits == 1
    assert len(cursor.executed_queries) == 1
    query, params = cursor.executed_queries[0]
    assert "INSERT INTO funnel_entries" in query
    assert params[0] == "user@example.com"
    assert params[1] == "language"
    assert params[2] == 10
    assert params[3] == 42


def test_mark_certificate_purchased_without_test_id_updates_and_commits():
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

    assert connection.commits == 1
    assert len(cursor.executed_queries) == 1
    query, params = cursor.executed_queries[0]
    assert "UPDATE funnel_entries" in query
    assert "WHERE email = %s" in query
    assert "AND funnel_type = %s" in query
    assert "AND test_id = %s" not in query
    assert params == (purchased_at, "user@example.com", "language")


def test_mark_certificate_purchased_with_test_id_updates_and_commits():
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

    assert connection.commits == 1
    assert len(cursor.executed_queries) == 1
    query, params = cursor.executed_queries[0]
    assert "UPDATE funnel_entries" in query
    assert "AND test_id = %s" in query
    assert params == (purchased_at, "user@example.com", "language", 42)
