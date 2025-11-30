"""MySQL test utilities for integration tests.

This module provides helper functions to set up test databases and apply
test schemas for integration testing without modifying production code.
"""

import logging
from pathlib import Path

import mysql.connector

from config.settings import DatabaseSettings

logger = logging.getLogger("tests.utils.mysql_test_utils")


def create_test_database(database_settings: DatabaseSettings) -> None:
    """Creates a test database if it does not exist.

    Connects to MySQL server without specifying a database name, then creates
    the database using the name and charset from database_settings.

    Args:
        database_settings: Configuration object containing database credentials
            and database name.

    Raises:
        mysql.connector.Error: If connection fails or database creation fails.
    """
    connection = mysql.connector.connect(
        host=database_settings.host,
        port=database_settings.port,
        user=database_settings.user,
        password=database_settings.password,
    )

    try:
        cursor = connection.cursor()
        query = (
            f"CREATE DATABASE IF NOT EXISTS {database_settings.name} "
            f"CHARACTER SET {database_settings.charset}"
        )
        cursor.execute(query)
        connection.commit()
        cursor.close()
        logger.info("Test database '%s' created or already exists", database_settings.name)
    finally:
        connection.close()


def apply_test_schema(database_settings: DatabaseSettings, schema_path: str) -> None:
    """Applies SQL schema from file to the test database.

    Reads SQL statements from the schema file, splits them by semicolon,
    and executes each non-empty statement in order.

    Args:
        database_settings: Configuration object containing database credentials.
        schema_path: Path to SQL schema file to apply.

    Raises:
        FileNotFoundError: If schema file does not exist.
        mysql.connector.Error: If database connection or SQL execution fails.
    """
    schema_file = Path(schema_path)
    if not schema_file.exists():
        raise FileNotFoundError(f"Schema file not found: {schema_path}")

    schema_content = schema_file.read_text(encoding="utf-8")

    connection = mysql.connector.connect(
        host=database_settings.host,
        port=database_settings.port,
        user=database_settings.user,
        password=database_settings.password,
        database=database_settings.name,
    )

    try:
        cursor = connection.cursor()

        # Split SQL by semicolon and execute each statement
        statements = schema_content.split(";")
        for statement in statements:
            statement = statement.strip()
            # Skip empty statements and comments-only statements
            if statement and not statement.startswith("--"):
                try:
                    cursor.execute(statement)
                except mysql.connector.Error as e:
                    logger.error("Failed to execute SQL statement: %s", statement[:100])
                    raise

        connection.commit()
        cursor.close()
        logger.info("Test schema applied successfully from %s", schema_path)
    finally:
        connection.close()

