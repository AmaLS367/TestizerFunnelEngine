import logging
from datetime import datetime
from typing import Optional

from mysql.connector import IntegrityError, MySQLConnection


def funnel_entry_exists(
    connection: MySQLConnection,
    email: str,
    funnel_type: str,
    test_id: Optional[int] = None,
) -> bool:
    """Checks if a funnel entry already exists for the given criteria.

    This is the primary idempotency check used throughout the system to prevent
    duplicate processing. When test_id is provided, matching is more specific,
    allowing the same email to exist in multiple funnels for different tests.

    Edge case: If test_id is None, matches any entry with the email+funnel_type
    combination, which may be too broad for some use cases but ensures no
    duplicates when test context is unavailable.

    Args:
        connection: Active MySQL database connection.
        email: User email address to check.
        funnel_type: Either 'language' or 'non_language'.
        test_id: Optional test identifier for more specific matching.
            If None, matches any entry regardless of test_id.

    Returns:
        True if matching entry exists, False otherwise.
    """
    cursor = connection.cursor()

    if test_id is None:
        query = """
        SELECT id
        FROM funnel_entries
        WHERE email = %s
          AND funnel_type = %s
        LIMIT 1
        """
        params = (email, funnel_type)
    else:
        query = """
        SELECT id
        FROM funnel_entries
        WHERE email = %s
          AND funnel_type = %s
          AND test_id = %s
        LIMIT 1
        """
        params = (email, funnel_type, test_id)  # type: ignore[assignment]

    cursor.execute(query, params)
    row = cursor.fetchone()
    cursor.close()

    return row is not None


def create_funnel_entry(
    connection: MySQLConnection,
    email: str,
    funnel_type: str,
    user_id: Optional[int] = None,
    test_id: Optional[int] = None,
) -> Optional[int]:
    """Records a new funnel entry when a user is added to a funnel.

    This function enforces idempotency at the database level using a unique constraint
    on (email, funnel_type, test_id). If a duplicate entry already exists, the function
    handles the IntegrityError gracefully by logging an informational message and
    returning None, without raising an exception.

    This function does not perform transaction control (commit/rollback). The caller
    is responsible for managing transactions.

    Args:
        connection: Active MySQL database connection.
        email: User email address.
        funnel_type: Either 'language' or 'non_language'.
        user_id: Optional MODX user ID for cross-referencing.
        test_id: Optional test ID for cross-referencing.

    Returns:
        The ID of the newly created funnel entry as an integer, or None if the entry
        already exists (duplicate detected via unique constraint).

    Raises:
        mysql.connector.Error: If database insert fails for reasons other than
            duplicate entry (e.g., connection error, other constraint violations).
    """
    logger = logging.getLogger("analytics.tracking")
    cursor = None

    try:
        cursor = connection.cursor()

        query = """
        INSERT INTO funnel_entries (
            email,
            funnel_type,
            user_id,
            test_id
        ) VALUES (%s, %s, %s, %s)
        """

        params = (email, funnel_type, user_id, test_id)

        cursor.execute(query, params)
        new_id = cursor.lastrowid
        return new_id

    except IntegrityError:
        # Handle duplicate entry due to unique constraint on (email, funnel_type, test_id)
        logger.info(
            "Duplicate funnel entry already exists (email=%s, funnel_type=%s, test_id=%s)",
            email,
            funnel_type,
            test_id,
        )
        return None

    finally:
        if cursor:
            cursor.close()


def mark_certificate_purchased(
    connection: MySQLConnection,
    email: str,
    funnel_type: str,
    test_id: Optional[int],
    purchased_at: datetime,
) -> None:
    """Updates funnel entries to mark certificate as purchased.

    Updates all matching entries (not just the first) to handle edge cases where
    the same user entered the funnel multiple times. The certificate_purchased=0
    condition ensures we only update entries that haven't been marked yet,
    making this operation idempotent.

    Edge case: If multiple entries match (same email+funnel_type), all are updated.
    This is intentional to handle historical duplicates without data loss.

    This function does not perform transaction control (commit/rollback). The caller
    is responsible for managing transactions.

    Args:
        connection: Active MySQL database connection.
        email: User email address to match.
        funnel_type: Either 'language' or 'non_language'.
        test_id: Optional test ID for more specific matching. If None, matches
            all entries regardless of test_id.
        purchased_at: Timestamp when purchase was completed. Used for conversion
            analytics and reporting.

    Raises:
        mysql.connector.Error: If database update fails (e.g., connection error).
    """
    cursor = connection.cursor()

    try:
        if test_id is None:
            query = """
            UPDATE funnel_entries
            SET certificate_purchased = 1,
                certificate_purchased_at = %s
            WHERE email = %s
              AND funnel_type = %s
              AND certificate_purchased = 0
            """
            params = (purchased_at, email, funnel_type)
        else:
            query = """
            UPDATE funnel_entries
            SET certificate_purchased = 1,
                certificate_purchased_at = %s
            WHERE email = %s
              AND funnel_type = %s
              AND test_id = %s
              AND certificate_purchased = 0
            """
            params = (purchased_at, email, funnel_type, test_id)  # type: ignore[assignment]

        cursor.execute(query, params)
    finally:
        cursor.close()
