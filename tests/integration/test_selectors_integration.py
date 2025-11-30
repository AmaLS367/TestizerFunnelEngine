"""Integration tests for database selectors using real MySQL database.

These tests verify that selectors work correctly with actual database queries
and data filtering logic.
"""

from datetime import datetime, timedelta

import pytest
from mysql.connector import MySQLConnection

from db.selectors import DEFAULT_CANDIDATE_LOOKBACK_DAYS, get_language_test_candidates


@pytest.mark.integration
def test_get_language_test_candidates_filters_correctly(
    mysql_test_connection: MySQLConnection,
) -> None:
    """Tests that get_language_test_candidates filters users correctly.

    Verifies that:
    - Only users with valid email and recent tests are returned
    - Users already in funnel_entries are excluded
    - Users outside lookback window are excluded
    - Users with missing/empty emails are excluded
    """
    cursor = mysql_test_connection.cursor()

    # Insert test data
    # Language
    cursor.execute("INSERT INTO simpletest_lang (Id) VALUES (1)")

    # Test linked to language
    cursor.execute("INSERT INTO simpletest_test (newid, Id, LangId) VALUES (1, 1, 1)")

    # Users with various scenarios
    now = datetime.now()
    recent_date = now - timedelta(days=10)  # Within lookback window
    old_date = now - timedelta(days=DEFAULT_CANDIDATE_LOOKBACK_DAYS + 10)  # Outside window

    # Valid candidate: recent test, valid email, not in funnel
    cursor.execute(
        "INSERT INTO simpletest_users (Id, Email, TestId, Datep, Status) VALUES (%s, %s, %s, %s, %s)",
        (1, "valid@example.com", 1, recent_date, 1),
    )

    # Valid candidate: recent test, valid email, not in funnel
    cursor.execute(
        "INSERT INTO simpletest_users (Id, Email, TestId, Datep, Status) VALUES (%s, %s, %s, %s, %s)",
        (2, "another@example.com", 1, recent_date, 1),
    )

    # Excluded: already in funnel_entries
    cursor.execute(
        "INSERT INTO simpletest_users (Id, Email, TestId, Datep, Status) VALUES (%s, %s, %s, %s, %s)",
        (3, "existing@example.com", 1, recent_date, 1),
    )
    cursor.execute(
        "INSERT INTO funnel_entries (email, funnel_type, user_id, test_id) VALUES (%s, %s, %s, %s)",
        ("existing@example.com", "language", 3, 1),
    )

    # Excluded: outside lookback window
    cursor.execute(
        "INSERT INTO simpletest_users (Id, Email, TestId, Datep, Status) VALUES (%s, %s, %s, %s, %s)",
        (4, "old@example.com", 1, old_date, 1),
    )

    # Excluded: missing email
    cursor.execute(
        "INSERT INTO simpletest_users (Id, Email, TestId, Datep, Status) VALUES (%s, %s, %s, %s, %s)",
        (5, None, 1, recent_date, 1),
    )

    # Excluded: empty email
    cursor.execute(
        "INSERT INTO simpletest_users (Id, Email, TestId, Datep, Status) VALUES (%s, %s, %s, %s, %s)",
        (6, "", 1, recent_date, 1),
    )

    mysql_test_connection.commit()
    cursor.close()

    # Call the selector
    candidates = get_language_test_candidates(mysql_test_connection, limit=100)

    # Assertions
    assert len(candidates) == 2, "Should return exactly 2 valid candidates"

    # Extract emails for easier checking
    candidate_emails = {email for _, email in candidates}
    assert "valid@example.com" in candidate_emails
    assert "another@example.com" in candidate_emails
    assert "existing@example.com" not in candidate_emails, "Should exclude users already in funnel"
    assert "old@example.com" not in candidate_emails, "Should exclude users outside lookback window"
    assert None not in candidate_emails, "Should exclude users with missing email"
    assert "" not in candidate_emails, "Should exclude users with empty email"


@pytest.mark.integration
def test_get_language_test_candidates_respects_limit(
    mysql_test_connection: MySQLConnection,
) -> None:
    """Tests that get_language_test_candidates respects the limit parameter."""
    cursor = mysql_test_connection.cursor()

    # Insert test data
    cursor.execute("INSERT INTO simpletest_lang (Id) VALUES (1)")
    cursor.execute("INSERT INTO simpletest_test (newid, Id, LangId) VALUES (1, 1, 1)")

    # Insert multiple valid candidates
    now = datetime.now()
    recent_date = now - timedelta(days=10)

    for i in range(10):
        cursor.execute(
            "INSERT INTO simpletest_users (Id, Email, TestId, Datep, Status) VALUES (%s, %s, %s, %s, %s)",
            (i + 1, f"user{i}@example.com", 1, recent_date, 1),
        )

    mysql_test_connection.commit()
    cursor.close()

    # Call with limit
    candidates = get_language_test_candidates(mysql_test_connection, limit=5)

    # Should return exactly the limit
    assert len(candidates) == 5


@pytest.mark.integration
def test_get_language_test_candidates_returns_empty_when_no_candidates(
    mysql_test_connection: MySQLConnection,
) -> None:
    """Tests that get_language_test_candidates returns empty list when no candidates exist."""
    # Call with empty database (already cleared by fixture)
    candidates = get_language_test_candidates(mysql_test_connection, limit=100)

    assert len(candidates) == 0

