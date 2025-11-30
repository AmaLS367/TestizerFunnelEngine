"""Pytest configuration and fixtures.

This module provides global fixtures and setup that applies to all tests.
It mocks mysql.connector to avoid import errors when the package is not installed.
"""

import sys
from unittest.mock import MagicMock


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

