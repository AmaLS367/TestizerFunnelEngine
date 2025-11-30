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

from config.settings import DatabaseSettings, Settings, load_settings
from db.connection import database_connection_scope
from tests.utils.mysql_test_utils import apply_test_schema, create_test_database


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

# Add mocks to sys.modules before any imports
sys.modules["mysql"] = _mysql_mock
sys.modules["mysql.connector"] = _mysql_connector_mock


@pytest.fixture
def test_settings() -> Settings:
    """Loads test settings with optional environment variable overrides.

    Loads settings using load_settings() and optionally overrides database
    attributes using environment variables:
    - TEST_DB_HOST → database.host
    - TEST_DB_PORT → database.port
    - TEST_DB_NAME → database.name

    Returns:
        Settings object with potentially overridden database configuration.
    """
    settings = load_settings()

    # Override database settings from environment variables if present
    if "TEST_DB_HOST" in os.environ:
        settings.database.host = os.environ["TEST_DB_HOST"]
    if "TEST_DB_PORT" in os.environ:
        try:
            settings.database.port = int(os.environ["TEST_DB_PORT"])
        except ValueError:
            pass  # Keep default if invalid
    if "TEST_DB_NAME" in os.environ:
        settings.database.name = os.environ["TEST_DB_NAME"]

    return settings


@pytest.fixture
def mysql_test_database(test_settings: Settings) -> DatabaseSettings:
    """Creates and prepares test database with schema.

    Creates the test database if it doesn't exist and applies the test schema.
    This fixture is the base for any integration test that needs a prepared
    database schema.

    Args:
        test_settings: Test settings fixture containing database configuration.

    Returns:
        DatabaseSettings object for the prepared test database.
    """
    create_test_database(test_settings.database)

    # Get absolute path to schema file
    schema_path = Path(__file__).parent / "mysql" / "test_schema.sql"
    apply_test_schema(test_settings.database, str(schema_path))

    return test_settings.database


@pytest.fixture
def mysql_test_connection(
    mysql_test_database: DatabaseSettings,
) -> Generator[MySQLConnection, None, None]:
    """Provides a clean MySQL connection for integration tests.

    Opens a database connection and clears all test tables before yielding.
    Ensures each test starts with a clean state.

    Args:
        mysql_test_database: Database settings fixture.

    Yields:
        Active MySQL connection object.
    """
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