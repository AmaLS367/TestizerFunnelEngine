"""Integration test for app.main in dry-run mode.

This test verifies that the main application entrypoint runs successfully
in dry-run mode without creating any database records.
"""

import os
from datetime import datetime, timedelta

import pytest
from mysql.connector import MySQLConnection


@pytest.mark.integration
def test_app_main_dry_run_does_not_create_records(
    mysql_test_connection: MySQLConnection,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Tests that app.main runs in dry-run mode without creating records.

    Sets APP_DRY_RUN=true, prepares minimal test data, runs main(),
    and verifies no records are created in funnel_entries or brevo_sync_outbox.
    """
    # Set dry-run mode
    monkeypatch.setenv("APP_DRY_RUN", "true")
    monkeypatch.setenv("BREVO_LANGUAGE_LIST_ID", "100")
    monkeypatch.setenv("BREVO_NON_LANGUAGE_LIST_ID", "0")

    # Insert minimal test data
    cursor = mysql_test_connection.cursor()

    cursor.execute("INSERT INTO simpletest_lang (Id) VALUES (1)")
    cursor.execute("INSERT INTO simpletest_test (Id, LangId) VALUES (1, 1)")

    now = datetime.now()
    recent_date = now - timedelta(days=10)

    cursor.execute(
        "INSERT INTO simpletest_users (Id, Email, TestId, Datep, Status) VALUES (%s, %s, %s, %s, %s)",
        (1, "dryrun@example.com", 1, recent_date, 1),
    )

    mysql_test_connection.commit()
    cursor.close()

    # Get initial counts
    cursor = mysql_test_connection.cursor()
    cursor.execute("SELECT COUNT(*) FROM funnel_entries")
    initial_funnel_count = cursor.fetchone()[0]

    cursor.execute("SELECT COUNT(*) FROM brevo_sync_outbox")
    initial_outbox_count = cursor.fetchone()[0]
    cursor.close()

    # Import and run main (this will use the monkeypatched environment)
    from app.main import main

    # Run main - should complete without exceptions
    try:
        main()
    except SystemExit:
        # main() may call sys.exit(), which is fine
        pass

    # Verify no records were created
    cursor = mysql_test_connection.cursor()
    cursor.execute("SELECT COUNT(*) FROM funnel_entries")
    final_funnel_count = cursor.fetchone()[0]

    cursor.execute("SELECT COUNT(*) FROM brevo_sync_outbox")
    final_outbox_count = cursor.fetchone()[0]
    cursor.close()

    assert final_funnel_count == initial_funnel_count, "No funnel entries should be created in dry-run"
    assert final_outbox_count == initial_outbox_count, "No outbox jobs should be created in dry-run"

