from __future__ import annotations

import os
import pathlib
import sys
from typing import NoReturn

from dotenv import load_dotenv

import mysql.connector

# Load environment variables from .env file
load_dotenv()

# Add project root to Python path for imports
project_root = pathlib.Path(__file__).resolve().parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))


def init_test_database() -> None:
    """Initialize test database and user for integration tests.

    This function creates a dedicated test database and user with full privileges,
    ensuring that integration tests and reset scripts never touch the production database.

    Reads the following environment variables from .env file:
    - TEST_DB_ADMIN_HOST: MySQL server host for admin connection
    - TEST_DB_ADMIN_PORT: MySQL server port for admin connection
    - TEST_DB_ADMIN_USER: MySQL admin username
    - TEST_DB_ADMIN_PASSWORD: MySQL admin password
    - TEST_DB_NAME: Name of the test database to create
    - TEST_DB_USER: Username for the test database user
    - TEST_DB_PASSWORD: Password for the test database user

    Raises:
        RuntimeError: If required environment variables are missing.
        mysql.connector.Error: If database operations fail.
    """
    # Read environment variables
    admin_host = os.getenv("TEST_DB_ADMIN_HOST")
    if not admin_host:
        raise RuntimeError("TEST_DB_ADMIN_HOST is not set")

    admin_port_str = os.getenv("TEST_DB_ADMIN_PORT")
    if not admin_port_str:
        raise RuntimeError("TEST_DB_ADMIN_PORT is not set")

    try:
        admin_port = int(admin_port_str)
    except ValueError:
        raise RuntimeError(f"Invalid TEST_DB_ADMIN_PORT value: {admin_port_str}")

    admin_user = os.getenv("TEST_DB_ADMIN_USER")
    if not admin_user:
        raise RuntimeError("TEST_DB_ADMIN_USER is not set")

    admin_password = os.getenv("TEST_DB_ADMIN_PASSWORD")
    if not admin_password:
        raise RuntimeError("TEST_DB_ADMIN_PASSWORD is not set")

    test_db_name = os.getenv("TEST_DB_NAME")
    if not test_db_name:
        raise RuntimeError("TEST_DB_NAME is not set")

    test_db_user = os.getenv("TEST_DB_USER")
    if not test_db_user:
        raise RuntimeError("TEST_DB_USER is not set")

    test_db_password = os.getenv("TEST_DB_PASSWORD")
    if not test_db_password:
        raise RuntimeError("TEST_DB_PASSWORD is not set")

    # Connect to MySQL as admin
    admin_connection = None
    try:
        admin_connection = mysql.connector.connect(
            host=admin_host,
            port=admin_port,
            user=admin_user,
            password=admin_password,
        )

        cursor = admin_connection.cursor()

        # Create test database if it does not exist
        create_db_query = (
            f"CREATE DATABASE IF NOT EXISTS {test_db_name} "
            f"CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci"
        )
        cursor.execute(create_db_query)
        print(f"Database '{test_db_name}' created or already exists")

        # Create test user if it does not exist
        # MySQL 8.0+ supports CREATE USER IF NOT EXISTS, but we handle errors for compatibility
        create_user_query = (
            f"CREATE USER IF NOT EXISTS '{test_db_user}'@'%' "
            f"IDENTIFIED BY '{test_db_password}'"
        )
        try:
            cursor.execute(create_user_query)
            print(f"User '{test_db_user}' created or already exists")
        except mysql.connector.Error as e:
            # If CREATE USER IF NOT EXISTS is not supported, try without IF NOT EXISTS
            # and ignore "user already exists" errors
            error_msg = str(e).lower()
            if "if not exists" in error_msg or "syntax" in error_msg:
                # Try without IF NOT EXISTS
                create_user_query_alt = (
                    f"CREATE USER '{test_db_user}'@'%' "
                    f"IDENTIFIED BY '{test_db_password}'"
                )
                try:
                    cursor.execute(create_user_query_alt)
                    print(f"User '{test_db_user}' created")
                except mysql.connector.Error as e2:
                    error_msg2 = str(e2).lower()
                    if "already exists" not in error_msg2:
                        raise
                    print(f"User '{test_db_user}' already exists")
            elif "already exists" not in error_msg:
                raise

        # Grant ALL PRIVILEGES on the test database to the test user
        grant_query = f"GRANT ALL PRIVILEGES ON {test_db_name}.* TO '{test_db_user}'@'%'"
        cursor.execute(grant_query)
        print(f"Granted ALL PRIVILEGES on '{test_db_name}' to '{test_db_user}'")

        # Flush privileges
        cursor.execute("FLUSH PRIVILEGES")
        print("Privileges flushed")

        cursor.close()
        admin_connection.commit()

    finally:
        if admin_connection and admin_connection.is_connected():
            admin_connection.close()


def main() -> NoReturn:
    """Entry point for manual runs."""
    init_test_database()
    sys.exit(0)


if __name__ == "__main__":
    main()

