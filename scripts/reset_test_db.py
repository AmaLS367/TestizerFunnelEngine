from __future__ import annotations

import pathlib
import sys
from typing import NoReturn

from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Add project root to Python path for imports
project_root = pathlib.Path(__file__).resolve().parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

from tests.utils.mysql_test_utils import (  # noqa: E402
    create_test_database,
    apply_test_schema,
)
from tests.utils.test_db_settings import get_test_database_settings  # noqa: E402


def reset_test_database() -> None:
    """Reset test database schema using test_schema.sql.

    This function recreates all required tables for integration tests.
    It relies on the schema file to drop and create tables in the correct order.

    Uses test database settings from TEST_DB_* environment variables to ensure
    it never operates on the production database.
    """
    database_settings = get_test_database_settings()

    # Ensure test database exists
    create_test_database(database_settings)

    # Apply test schema (DROP TABLE IF EXISTS + CREATE TABLE ...)
    schema_path = project_root / "tests" / "mysql" / "test_schema.sql"

    apply_test_schema(database_settings, str(schema_path))


def main() -> NoReturn:
    """Entry point for manual runs."""
    reset_test_database()
    sys.exit(0)


if __name__ == "__main__":
    main()
