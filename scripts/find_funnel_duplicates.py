"""Read-only script to detect duplicate funnel entries before applying unique constraint.

This script queries the funnel_entries table to find groups of entries that share
the same (email, funnel_type, test_id) combination. It is designed to help operators
identify duplicates before running the migration that adds a unique constraint.

The script is strictly read-only and does not perform any DELETE, UPDATE, or INSERT
operations.
"""

import os
import sys
from typing import Any, List, Tuple

import mysql.connector
from mysql.connector import MySQLConnection


def load_database_settings() -> Tuple[str, int, str, str, str, str]:
    """Load database connection settings from environment variables.

    Returns:
        Tuple of (host, port, user, password, database_name, charset)
    """
    host = os.getenv("DB_HOST", "127.0.0.1")
    port_str = os.getenv("DB_PORT", "3306")
    try:
        port = int(port_str)
    except ValueError:
        port = 3306
    user = os.getenv("DB_USER", "testizer_user")
    password = os.getenv("DB_PASSWORD", "change_me")
    database_name = os.getenv("DB_NAME", "testizer")
    charset = os.getenv("DB_CHARSET", "utf8mb4")

    return (host, port, user, password, database_name, charset)


def create_connection() -> MySQLConnection:
    """Create a MySQL database connection using environment variables.

    Returns:
        Active MySQL connection object.

    Raises:
        mysql.connector.Error: If connection fails.
    """
    host, port, user, password, database_name, charset = load_database_settings()

    connection = mysql.connector.connect(
        host=host,
        port=port,
        user=user,
        password=password,
        database=database_name,
        charset=charset,
    )

    return connection  # type: ignore[return-value]


def find_duplicate_entries(connection: MySQLConnection) -> List[Tuple[Any, ...]]:
    """Find duplicate funnel entries grouped by (email, funnel_type, test_id).

    Args:
        connection: Active MySQL connection.

    Returns:
        List of tuples containing:
        (email, funnel_type, test_id, count, min_id, max_id, min_entered_at, max_entered_at)
        where test_id and datetime fields may be None.
    """
    query = """
        SELECT
            email,
            funnel_type,
            test_id,
            COUNT(*) as count,
            MIN(id) as min_id,
            MAX(id) as max_id,
            MIN(entered_at) as min_entered_at,
            MAX(entered_at) as max_entered_at
        FROM funnel_entries
        GROUP BY email, funnel_type, test_id
        HAVING COUNT(*) > 1
        ORDER BY count DESC, email, funnel_type, test_id
    """

    cursor = connection.cursor(dictionary=False)
    cursor.execute(query)
    results = cursor.fetchall()
    cursor.close()

    return results


def format_results(results: List[Tuple[Any, ...]]) -> str:
    """Format duplicate entries as a readable table.

    Args:
        results: List of duplicate entry tuples.

    Returns:
        Formatted string table.
    """
    if not results:
        return "No duplicate entries found.\n"

    # Calculate column widths
    max_email_len = max(len(str(row[0])) for row in results)
    max_funnel_type_len = max(len(str(row[1])) for row in results)
    max_test_id_len = max(len(str(row[2])) if row[2] is not None else 4 for row in results)

    # Ensure minimum widths for headers
    email_width = max(max_email_len, len("Email"))
    funnel_type_width = max(max_funnel_type_len, len("Funnel Type"))
    test_id_width = max(max_test_id_len, len("Test ID"))

    # Build header
    header = (
        f"{'Email':<{email_width}} | "
        f"{'Funnel Type':<{funnel_type_width}} | "
        f"{'Test ID':<{test_id_width}} | "
        f"{'Count':<6} | "
        f"{'Min ID':<8} | "
        f"{'Max ID':<8} | "
        f"{'Min Entered At':<20} | "
        f"{'Max Entered At':<20}"
    )
    separator = "-" * len(header)

    lines = [header, separator]

    # Build rows
    for row in results:
        email, funnel_type, test_id, count, min_id, max_id, min_entered_at, max_entered_at = row
        test_id_str = str(test_id) if test_id is not None else "NULL"
        min_entered_str = str(min_entered_at) if min_entered_at else "NULL"
        max_entered_str = str(max_entered_at) if max_entered_at else "NULL"

        line = (
            f"{str(email):<{email_width}} | "
            f"{str(funnel_type):<{funnel_type_width}} | "
            f"{test_id_str:<{test_id_width}} | "
            f"{count:<6} | "
            f"{min_id:<8} | "
            f"{max_id:<8} | "
            f"{min_entered_str:<20} | "
            f"{max_entered_str:<20}"
        )
        lines.append(line)

    return "\n".join(lines) + "\n"


def main() -> None:
    """Main entry point for the duplicate detection script."""
    connection = None
    try:
        connection = create_connection()
        duplicates = find_duplicate_entries(connection)

        print(f"Found {len(duplicates)} duplicate group(s):\n")
        print(format_results(duplicates))

        if duplicates:
            print(
                "\nNote: These duplicates must be cleaned manually or via MySQL scripts "
                "before running the migration that adds the unique constraint."
            )

    except mysql.connector.Error as e:
        print(f"Database error: {e}", file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        print(f"Unexpected error: {e}", file=sys.stderr)
        sys.exit(1)
    finally:
        if connection:
            connection.close()

    # Exit with code 0 even if duplicates are found (diagnostic script)
    sys.exit(0)


if __name__ == "__main__":
    main()
