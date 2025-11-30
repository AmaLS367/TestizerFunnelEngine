from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

from mysql.connector import MySQLConnection


DEFAULT_CANDIDATE_LOOKBACK_DAYS = 30


def get_language_test_candidates(
    connection: MySQLConnection,
    limit: int = 100,
) -> List[Tuple[int, str]]:
    """Fetches users who completed language tests and are eligible for funnel entry.

    Selects users from simpletest tables who:
    - Completed a language test within the lookback window
    - Have a valid email address
    - Are not already in the language funnel (idempotency check)

    The LEFT JOIN with funnel_entries ensures we never process the same user twice,
    preventing duplicate Brevo API calls and maintaining data integrity.

    Args:
        connection: Active MySQL database connection.
        limit: Maximum number of candidates to return. Used for batch processing
            to manage memory footprint during high-load periods.

    Returns:
        List of tuples containing (user_id, email) for eligible candidates.
        Empty list if no candidates found or all users already processed.
    """
    cursor = connection.cursor()

    query = """
    SELECT
        u.Id AS user_id,
        u.Email AS email
    FROM simpletest_users AS u
    INNER JOIN simpletest_test AS t
        ON t.Id = u.TestId
    INNER JOIN simpletest_lang AS l
        ON l.Id = t.LangId
    LEFT JOIN funnel_entries AS f
        ON f.email = u.Email
       AND f.funnel_type = %s
    WHERE
        u.Email IS NOT NULL
        AND u.Email <> ''
        AND u.Datep >= DATE_SUB(NOW(), INTERVAL %s DAY)
        AND f.id IS NULL
    ORDER BY
        u.Datep DESC
    LIMIT %s
    """

    params = ("language", DEFAULT_CANDIDATE_LOOKBACK_DAYS, limit)

    cursor.execute(query, params)
    rows = cursor.fetchall()
    cursor.close()

    return rows  # type: ignore[no-any-return]  # type: ignore[return-value]


def get_non_language_test_candidates(
    connection: MySQLConnection,
    limit: int = 100,
) -> List[Tuple[int, str]]:
    """Placeholder for non-language test candidate selection.

    Currently returns empty list as non-language funnel logic is not yet implemented.
    This maintains API compatibility while allowing future expansion.

    Args:
        connection: Active MySQL database connection (unused currently).
        limit: Maximum number of candidates to return (unused currently).

    Returns:
        Empty list until non-language test selection logic is implemented.
    """
    return []


def get_pending_funnel_entries(
    connection: MySQLConnection,
    max_rows: int = 100,
) -> List[Tuple[str, str, Optional[int], Optional[int]]]:
    """Retrieves funnel entries that haven't been marked as purchased.

    Used by purchase synchronization to identify users who entered the funnel
    but haven't yet purchased certificates. Entries are ordered by entry time
    to process oldest first, ensuring fair processing order.

    Args:
        connection: Active MySQL database connection.
        max_rows: Maximum number of entries to process per batch. Prevents
            memory exhaustion when large backlogs exist.

    Returns:
        List of tuples: (email, funnel_type, user_id, test_id).
        Empty list if all entries are already marked as purchased.
    """
    cursor = connection.cursor()

    query = """
    SELECT
        email,
        funnel_type,
        user_id,
        test_id
    FROM funnel_entries
    WHERE certificate_purchased = 0
    ORDER BY entered_at ASC
    LIMIT %s
    """

    cursor.execute(query, (max_rows,))
    rows = cursor.fetchall()
    cursor.close()

    return rows  # type: ignore[no-any-return,return-value]


def get_certificate_purchase_for_entry(
    connection: MySQLConnection,
    email: str,
    funnel_type: str,
    user_id: Optional[int],
    test_id: Optional[int],
) -> Optional[Tuple[int, datetime]]:
    """Checks MODX certificate tables for completed purchases matching funnel entry.

    Queries modx_cert_payment to find paid certificates (id_status=2) that match
    the user's email and funnel type. The funnel_type determines which test type
    to match (language=1, non_language=2).

    Note: user_id and test_id parameters are accepted for API compatibility but
    not used in the query. Matching is performed by email and funnel type only.

    Args:
        connection: Active MySQL database connection.
        email: User email address to search for.
        funnel_type: Either 'language' or 'non_language' to filter test type.
        user_id: Unused parameter maintained for API compatibility.
        test_id: Unused parameter maintained for API compatibility.

    Returns:
        Tuple of (payment_id, payment_datetime) if purchase found, None otherwise.
        Payment datetime is returned as datetime object for direct use in updates.
    """
    query = """
    SELECT
        p.id,
        p.datetime_payment
    FROM modx_cert_payment AS p
    INNER JOIN modx_cert_result AS r ON r.id = p.id_result
    INNER JOIN modx_cert_users AS u ON u.id = r.id_user
    INNER JOIN modx_cert_test AS t ON t.id = r.id_test
    WHERE
        u.email = %(email)s
        AND p.id_status = 2
        AND p.datetime_payment IS NOT NULL
        AND (
            (%(funnel_type)s = 'language' AND t.type = 1)
            OR (%(funnel_type)s = 'non_language' AND t.type = 2)
        )
    ORDER BY p.datetime_payment ASC
    LIMIT 1
    """

    params = {
        "email": email,
        "funnel_type": funnel_type,
    }

    with connection.cursor() as cursor:
        cursor.execute(query, params)  # type: ignore[arg-type]
        row = cursor.fetchone()

    if row is None:
        return None

    payment_id, payment_datetime = row

    return int(payment_id), payment_datetime  # type: ignore[return-value, arg-type]


def get_funnel_conversion_summary(
    connection: MySQLConnection,
    from_date: Optional[datetime],
    to_date: Optional[datetime],
) -> List[Tuple[str, int, int]]:
    """Aggregates funnel entry statistics grouped by funnel type.

    Calculates total entries and purchases per funnel type within optional date range.
    Used for conversion reporting without requiring full table scans.

    Edge case: Returns empty list if no entries exist, allowing callers to handle
    zero-state gracefully without special null checks.

    Args:
        connection: Active MySQL database connection.
        from_date: Start of reporting period (inclusive). None means no lower bound.
        to_date: End of reporting period (exclusive). None means no upper bound.

    Returns:
        List of tuples: (funnel_type, total_entries, total_purchased).
        Results are ordered by funnel_type for consistent reporting output.
    """
    query = """
    SELECT
        funnel_type,
        COUNT(*) AS total_entries,
        SUM(CASE WHEN certificate_purchased = 1 THEN 1 ELSE 0 END) AS total_purchased
    FROM funnel_entries
    WHERE 1 = 1
    """

    params: Dict[str, Any] = {}

    if from_date is not None:
        query += " AND entered_at >= %(from_date)s"
        params["from_date"] = from_date

    if to_date is not None:
        query += " AND entered_at < %(to_date)s"
        params["to_date"] = to_date

    query += " GROUP BY funnel_type ORDER BY funnel_type"

    with connection.cursor() as cursor:
        cursor.execute(query, params)
        rows = cursor.fetchall()

    result = []

    for row in rows:
        funnel_type, total_entries, total_purchased = row
        result.append(
            (
                str(funnel_type),
                int(total_entries),  # type: ignore[arg-type]
                int(total_purchased),  # type: ignore[arg-type]
            )
        )

    return result
