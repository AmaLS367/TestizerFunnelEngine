from contextlib import contextmanager
from typing import Generator

import mysql.connector
from mysql.connector import MySQLConnection

from config.settings import DatabaseSettings


def create_database_connection(database_settings: DatabaseSettings) -> MySQLConnection:
    """Creates a new MySQL database connection.

    Establishes a connection to the MODX database using credentials from settings.
    The connection must be explicitly closed by the caller to prevent resource leaks.

    Args:
        database_settings: Configuration object containing database credentials.

    Returns:
        Active MySQL connection object. Caller is responsible for closing it.

    Raises:
        mysql.connector.Error: If connection fails due to invalid credentials,
            network issues, or database unavailability.
    """
    connection = mysql.connector.connect(
        host=database_settings.host,
        port=database_settings.port,
        user=database_settings.user,
        password=database_settings.password,
        database=database_settings.name,
        charset=database_settings.charset,
    )

    return connection  # type: ignore[return-value]


@contextmanager
def database_connection_scope(
    database_settings: DatabaseSettings,
) -> Generator[MySQLConnection, None, None]:
    """Context manager for database connections with automatic cleanup.

    Ensures connection is properly closed even if exceptions occur during execution.
    This prevents connection pool exhaustion and database lock timeouts.

    Args:
        database_settings: Configuration object containing database credentials.

    Yields:
        Active MySQL connection object.

    Example:
        ```python
        with database_connection_scope(settings.database) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT 1")
        # Connection automatically closed here
        ```
    """
    connection = create_database_connection(database_settings)

    try:
        yield connection
    finally:
        connection.close()
