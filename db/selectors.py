from typing import List, Tuple, Optional

from mysql.connector import MySQLConnection


DEFAULT_CANDIDATE_LOOKBACK_DAYS = 30


def get_language_test_candidates(
    connection: MySQLConnection,
    limit: int = 100,
) -> List[Tuple[int, str]]:
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

    return rows


def get_non_language_test_candidates(
    connection: MySQLConnection,
    limit: int = 100,
) -> List[Tuple[int, str]]:
    return []


def get_pending_funnel_entries(
    connection: MySQLConnection,
    max_rows: int = 100,
) -> List[Tuple]:
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

    return rows


def get_certificate_purchase_for_entry(
    connection: MySQLConnection,
    email: str,
    user_id: Optional[int],
    test_id: Optional[int],
) -> Optional[Tuple]:
    cursor = connection.cursor()

    query = """
    SELECT
        o.id AS order_id,
        o.created_at AS purchased_at
    FROM orders AS o
    WHERE
        o.email = %s
        AND o.is_certificate = 1
        AND o.status IN ('paid', 'completed')
    ORDER BY o.created_at DESC
    LIMIT 1
    """

    params = (email,)

    cursor.execute(query, params)
    row = cursor.fetchone()
    cursor.close()

    return row
