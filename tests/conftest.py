"""Pytest configuration and fixtures.

This module provides global fixtures and setup that applies to all tests.
It mocks mysql.connector to avoid import errors when the package is not installed.
"""

import os
import sys
from pathlib import Path
from typing import Generator
from unittest.mock import MagicMock

import pytest
from mysql.connector import MySQLConnection

from config.settings import DatabaseSettings, Settings
from db.connection import database_connection_scope
from tests.utils.mysql_test_utils import apply_test_schema, create_test_database
from tests.utils.test_db_settings import get_test_database_settings


def pytest_configure(config):
    """Register custom pytest markers."""
    config.addinivalue_line(
        "markers",
        "integration: marks tests that require a real MySQL database",
    )


class MockMySQLConnection:
    """Mock MySQLConnection class."""

    pass


class MockError(Exception):
    """Mock mysql.connector.Error."""

    pass


class MockIntegrityError(MockError):
    """Mock mysql.connector.IntegrityError."""

    pass


# Create mock mysql module structure
_mysql_mock = MagicMock()
_mysql_connector_mock = MagicMock()

# Set up the connector mock with required classes
_mysql_connector_mock.Error = MockError
_mysql_connector_mock.IntegrityError = MockIntegrityError
_mysql_connector_mock.MySQLConnection = MockMySQLConnection
_mysql_connector_mock.connect = MagicMock()

# Set up the mysql module
_mysql_mock.connector = _mysql_connector_mock

# Store original modules if they exist
_original_mysql = sys.modules.get("mysql")
_original_mysql_connector = sys.modules.get("mysql.connector")

# Add mocks to sys.modules before any imports
# Note: For integration tests, we need to restore the real modules
sys.modules["mysql"] = _mysql_mock
sys.modules["mysql.connector"] = _mysql_connector_mock


@pytest.fixture
def test_settings() -> Settings:
    """Loads test settings with test database configuration.

    Uses get_test_database_settings() to ensure integration tests always use
    a dedicated test database configured via TEST_DB_* environment variables,
    never the production database.

    Returns:
        Settings object with test database configuration.
    """
    from config.settings import (
        ApplicationSettings,
        BrevoSettings,
        SentrySettings,
        load_settings,
    )

    settings = load_settings()
    # Replace database settings with test database settings
    settings.database = get_test_database_settings()

    return settings


@pytest.fixture
def mysql_test_database(test_settings: Settings) -> DatabaseSettings:
    """Creates and prepares test database with schema.

    For DB integration tests, this fixture restores the real mysql.connector
    and prepares the test database. If TESTIZER_ENABLE_DB_TESTS is not "1",
    the tests using this fixture are skipped.

    Args:
        test_settings: Test settings fixture containing database configuration.

    Returns:
        DatabaseSettings object for the prepared test database.
    """
    if os.getenv("TESTIZER_ENABLE_DB_TESTS", "0") != "1":
        pytest.skip(
            "DB integration tests are disabled. "
            "Set TESTIZER_ENABLE_DB_TESTS=1 and configure a test database to run them."
        )

    # Restore real mysql.connector for integration tests
    import importlib

    # Remove mocks and restore real modules
    if "mysql" in sys.modules:
        del sys.modules["mysql"]
    if "mysql.connector" in sys.modules:
        del sys.modules["mysql.connector"]

    # Import real mysql.connector
    import mysql.connector  # noqa: F401

    # Reload modules that use mysql.connector
    if "tests.utils.mysql_test_utils" in sys.modules:
        importlib.reload(sys.modules["tests.utils.mysql_test_utils"])

    try:
        create_test_database(test_settings.database)

        # Get absolute path to schema file
        schema_path = Path(__file__).parent / "mysql" / "test_schema.sql"
        apply_test_schema(test_settings.database, str(schema_path))

        return test_settings.database
    finally:
        # Restore mocks after database setup
        sys.modules["mysql"] = _mysql_mock
        sys.modules["mysql.connector"] = _mysql_connector_mock
        # Reload modules to use mocks again
        if "tests.utils.mysql_test_utils" in sys.modules:
            importlib.reload(sys.modules["tests.utils.mysql_test_utils"])


@pytest.fixture
def mysql_test_connection(
    mysql_test_database: DatabaseSettings,
) -> Generator[MySQLConnection, None, None]:
    """Provides a clean MySQL connection for integration tests.

    Opens a database connection and clears all test tables before yielding.
    Ensures each test starts with a clean state.

    For integration tests, this fixture restores the real mysql.connector module
    to allow actual database connections.

    Args:
        mysql_test_database: Database settings fixture.

    Yields:
        Active MySQL connection object.
    """
    # Restore real mysql.connector for integration tests
    # (mysql_test_database already restored it, but we ensure it's still real)
    import importlib

    # Remove mocks and restore real modules
    if "mysql" in sys.modules:
        del sys.modules["mysql"]
    if "mysql.connector" in sys.modules:
        del sys.modules["mysql.connector"]

    # Import real mysql.connector
    import mysql.connector  # noqa: F401

    # Reload modules that use mysql.connector
    if "db.connection" in sys.modules:
        importlib.reload(sys.modules["db.connection"])
    if "tests.utils.mysql_test_utils" in sys.modules:
        importlib.reload(sys.modules["tests.utils.mysql_test_utils"])

    try:
        with database_connection_scope(mysql_test_database) as connection:
            cursor = connection.cursor()

            # Clear all test tables
            cursor.execute("DELETE FROM brevo_sync_outbox")
            cursor.execute("DELETE FROM funnel_entries")
            cursor.execute("DELETE FROM simpletest_users")
            cursor.execute("DELETE FROM simpletest_test")
            cursor.execute("DELETE FROM simpletest_lang")

            cursor.close()
            connection.commit()

            yield connection
    finally:
        # Restore mocks after integration test
        sys.modules["mysql"] = _mysql_mock
        sys.modules["mysql.connector"] = _mysql_connector_mock
        # Reload modules to use mocks again
        if "db.connection" in sys.modules:
            importlib.reload(sys.modules["db.connection"])
        if "tests.utils.mysql_test_utils" in sys.modules:
            importlib.reload(sys.modules["tests.utils.mysql_test_utils"])
