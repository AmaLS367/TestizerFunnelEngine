"""Helper functions for test database configuration.

This module provides utilities to build test database settings from environment
variables, ensuring that integration tests always use a dedicated test database
and never accidentally connect to the production database.
"""

import os

from config.settings import DatabaseSettings, load_settings


def get_test_database_settings() -> DatabaseSettings:
    """Build test database settings from environment variables.

    Starts with the base settings from load_settings() and overrides database
    configuration with TEST_DB_* environment variables if they are set.

    Environment variables:
    - TEST_DB_HOST: Overrides database host (default: from base settings)
    - TEST_DB_PORT: Overrides database port (default: from base settings)
    - TEST_DB_NAME: Overrides database name (required for safety)
    - TEST_DB_USER: Overrides database user (default: from base settings)
    - TEST_DB_PASSWORD: Overrides database password (default: from base settings)

    Returns:
        DatabaseSettings instance configured for test database.

    Raises:
        RuntimeError: If TEST_DB_NAME is not set, to prevent accidental use
            of production database.
    """
    settings = load_settings()
    database = settings.database

    # TEST_DB_NAME must be set to prevent accidental use of production database
    test_db_name = os.getenv("TEST_DB_NAME")
    if not test_db_name:
        raise RuntimeError(
            "TEST_DB_NAME must be set for integration tests. "
            "This prevents accidental use of the production database."
        )

    # Override database settings from environment variables if present
    host = os.getenv("TEST_DB_HOST", database.host)
    port_str = os.getenv("TEST_DB_PORT")
    if port_str:
        try:
            port = int(port_str)
        except ValueError:
            port = database.port
    else:
        port = database.port
    user = os.getenv("TEST_DB_USER", database.user)
    password = os.getenv("TEST_DB_PASSWORD", database.password)
    charset = database.charset  # Keep charset from base settings

    return DatabaseSettings(
        host=host,
        port=port,
        user=user,
        password=password,
        name=test_db_name,
        charset=charset,
    )

