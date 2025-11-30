from datetime import datetime
from typing import Any, List, Optional, Tuple

from db import selectors


class DummyCursor:
    def __init__(
        self,
        rows: Optional[List[Tuple[Any, ...]]] = None,
        row: Optional[Tuple[Any, ...]] = None,
    ) -> None:
        self.rows = rows or []
        self.row = row
        self.last_query: Optional[str] = None
        self.last_params: Optional[Tuple[Any, ...]] = None
        self.closed = False
        self.fetchall_called = False
        self.fetchone_called = False

    def execute(self, query: str, params: Any = None) -> None:
        self.last_query = query
        self.last_params = params

    def fetchall(self) -> List[Tuple[Any, ...]]:
        self.fetchall_called = True
        return list(self.rows)

    def fetchone(self) -> Optional[Tuple[Any, ...]]:
        self.fetchone_called = True
        return self.row

    def close(self) -> None:
        self.closed = True

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
        return False


class DummyConnection:
    def __init__(self, cursor: DummyCursor) -> None:
        self._cursor = cursor
        self.cursor_called = False

    def cursor(self) -> DummyCursor:
        self.cursor_called = True
        return self._cursor


def test_get_language_test_candidates_uses_params_and_returns_rows() -> None:
    expected_rows = [(1, "user1@example.com"), (2, "user2@example.com")]
    dummy_cursor = DummyCursor(rows=expected_rows)
    connection = DummyConnection(cursor=dummy_cursor)

    result = selectors.get_language_test_candidates(connection, limit=50)  # type: ignore[arg-type]

    assert result == expected_rows
    assert dummy_cursor.fetchall_called is True
    assert dummy_cursor.closed is True
    assert dummy_cursor.last_params is not None
    assert dummy_cursor.last_params[0] == "language"
    assert dummy_cursor.last_params[2] == 50


def test_get_non_language_test_candidates_is_placeholder() -> None:
    dummy_cursor = DummyCursor()
    connection = DummyConnection(cursor=dummy_cursor)

    result = selectors.get_non_language_test_candidates(connection, limit=25)  # type: ignore[arg-type]

    assert result == []


def test_get_pending_funnel_entries_uses_max_rows_parameter() -> None:
    expected_rows = [("user@example.com", "language", 10, 20)]
    dummy_cursor = DummyCursor(rows=expected_rows)
    connection = DummyConnection(cursor=dummy_cursor)

    result = selectors.get_pending_funnel_entries(connection, max_rows=40)  # type: ignore[arg-type]

    assert result == expected_rows
    assert dummy_cursor.fetchall_called is True
    assert dummy_cursor.closed is True
    assert dummy_cursor.last_params == (40,)


def test_get_certificate_purchase_for_entry_returns_row_or_none() -> None:
    payment_datetime = datetime(2025, 1, 1, 12, 0, 0)

    dummy_cursor_with_row = DummyCursor(row=(123, payment_datetime))
    connection_with_row = DummyConnection(cursor=dummy_cursor_with_row)

    found = selectors.get_certificate_purchase_for_entry(
        connection=connection_with_row,  # type: ignore[arg-type]
        email="user@example.com",
        funnel_type="language",
        user_id=1,
        test_id=2,
    )

    assert found == (123, payment_datetime)
    assert dummy_cursor_with_row.fetchone_called is True
    assert dummy_cursor_with_row.closed is True

    dummy_cursor_without_row = DummyCursor(row=None)
    connection_without_row = DummyConnection(cursor=dummy_cursor_without_row)

    not_found = selectors.get_certificate_purchase_for_entry(
        connection=connection_without_row,  # type: ignore[arg-type]
        email="user@example.com",
        funnel_type="language",
        user_id=1,
        test_id=2,
    )

    assert not_found is None
    assert dummy_cursor_without_row.fetchone_called is True
    assert dummy_cursor_without_row.closed is True
