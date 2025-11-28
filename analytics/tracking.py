from typing import Optional

from mysql.connector import MySQLConnection


def funnel_entry_exists(
    connection: MySQLConnection,
    email: str,
    funnel_type: str,
    test_id: Optional[int] = None,
) -> bool:
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
        params = (email, funnel_type, test_id)

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
) -> None:
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
    connection.commit()
    cursor.close()

