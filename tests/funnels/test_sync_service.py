from funnels.sync_service import FunnelSyncService
from brevo.models import BrevoContact


class DummyBrevoClient:
    def __init__(self):
        self.calls = []

    def create_or_update_contact(self, contact: BrevoContact):
        self.calls.append(contact)


class DummyConnection:
    def __init__(self):
        self.transactions_started = 0
        self.commits = 0
        self.rollbacks = 0

    def start_transaction(self):
        self.transactions_started += 1

    def commit(self):
        self.commits += 1

    def rollback(self):
        self.rollbacks += 1


def test_funnel_sync_creates_entries_and_enqueues_outbox_jobs(monkeypatch):
    """Test that in production mode, funnel entries are created and outbox jobs are enqueued."""
    language_candidates = [
        (1, "lang1@example.com"),
        (2, "lang2@example.com"),
    ]

    non_language_candidates = [
        (3, "non1@example.com"),
    ]

    created_entries = []
    enqueued_jobs = []
    entry_id_counter = 100

    def fake_get_language_test_candidates(connection, limit):
        return language_candidates

    def fake_get_non_language_test_candidates(connection, limit):
        return non_language_candidates

    def fake_create_funnel_entry(
        connection, email, funnel_type, user_id=None, test_id=None
    ):
        nonlocal entry_id_counter
        entry_id_counter += 1
        created_entries.append(
            {
                "email": email,
                "funnel_type": funnel_type,
                "user_id": user_id,
                "test_id": test_id,
            }
        )
        return entry_id_counter

    def fake_enqueue_brevo_sync_job(
        connection, funnel_entry_id, operation_type, payload
    ):
        enqueued_jobs.append(
            {
                "funnel_entry_id": funnel_entry_id,
                "operation_type": operation_type,
                "payload": payload,
            }
        )
        return 999  # outbox job ID

    import funnels.sync_service as sync_module

    monkeypatch.setattr(
        sync_module,
        "get_language_test_candidates",
        fake_get_language_test_candidates,
    )
    monkeypatch.setattr(
        sync_module,
        "get_non_language_test_candidates",
        fake_get_non_language_test_candidates,
    )
    monkeypatch.setattr(
        sync_module,
        "create_funnel_entry",
        fake_create_funnel_entry,
    )
    monkeypatch.setattr(
        sync_module,
        "enqueue_brevo_sync_job",
        fake_enqueue_brevo_sync_job,
    )

    brevo_client = DummyBrevoClient()
    connection = DummyConnection()

    service = FunnelSyncService(
        connection=connection,  # type: ignore[arg-type]
        brevo_client=brevo_client,  # type: ignore[arg-type]
        language_list_id=4,
        non_language_list_id=5,
        dry_run=False,
    )

    service.sync()

    # Should have created 3 funnel entries
    assert len(created_entries) == 3
    # Should have enqueued 3 outbox jobs
    assert len(enqueued_jobs) == 3
    # Should not have called Brevo API directly (only through outbox)
    assert len(brevo_client.calls) == 0
    # Should have started and committed 3 transactions
    assert connection.transactions_started == 3
    assert connection.commits == 3
    assert connection.rollbacks == 0

    # Verify outbox jobs have correct operation type
    for job in enqueued_jobs:
        assert job["operation_type"] == "upsert_contact"
        assert job["funnel_entry_id"] > 100  # Should have valid entry ID


def test_funnel_sync_does_nothing_when_no_candidates(monkeypatch):
    def fake_get_language_test_candidates(connection, limit):
        return []

    def fake_get_non_language_test_candidates(connection, limit):
        return []

    def fake_create_funnel_entry(
        connection, email, funnel_type, user_id=None, test_id=None
    ):
        raise AssertionError(
            "create_funnel_entry must not be called when there are no candidates"
        )

    import funnels.sync_service as sync_module

    monkeypatch.setattr(
        sync_module,
        "get_language_test_candidates",
        fake_get_language_test_candidates,
    )
    monkeypatch.setattr(
        sync_module,
        "get_non_language_test_candidates",
        fake_get_non_language_test_candidates,
    )
    monkeypatch.setattr(
        sync_module,
        "create_funnel_entry",
        fake_create_funnel_entry,
    )

    brevo_client = DummyBrevoClient()
    connection = DummyConnection()

    service = FunnelSyncService(
        connection=connection,  # type: ignore[arg-type]
        brevo_client=brevo_client,  # type: ignore[arg-type]
        language_list_id=4,
        non_language_list_id=5,
        dry_run=False,
    )

    service.sync()

    assert len(brevo_client.calls) == 0
    assert connection.transactions_started == 0


def test_funnel_sync_dry_run_does_not_call_brevo_or_create_entries(monkeypatch):
    """Test that dry-run mode does not call Brevo API or create funnel entries."""
    language_candidates = [
        (1, "lang1@example.com"),
    ]

    non_language_candidates = [
        (2, "non1@example.com"),
    ]

    created_entries = []
    enqueued_jobs = []

    def fake_get_language_test_candidates(connection, limit):
        return language_candidates

    def fake_get_non_language_test_candidates(connection, limit):
        return non_language_candidates

    def fake_create_funnel_entry(
        connection, email, funnel_type, user_id=None, test_id=None
    ):
        created_entries.append(
            {
                "email": email,
                "funnel_type": funnel_type,
                "user_id": user_id,
                "test_id": test_id,
            }
        )
        raise AssertionError(
            "create_funnel_entry must not be called in dry-run mode"
        )

    def fake_enqueue_brevo_sync_job(
        connection, funnel_entry_id, operation_type, payload
    ):
        enqueued_jobs.append(
            {
                "funnel_entry_id": funnel_entry_id,
                "operation_type": operation_type,
            }
        )
        raise AssertionError(
            "enqueue_brevo_sync_job must not be called in dry-run mode"
        )

    import funnels.sync_service as sync_module

    monkeypatch.setattr(
        sync_module,
        "get_language_test_candidates",
        fake_get_language_test_candidates,
    )
    monkeypatch.setattr(
        sync_module,
        "get_non_language_test_candidates",
        fake_get_non_language_test_candidates,
    )
    monkeypatch.setattr(
        sync_module,
        "create_funnel_entry",
        fake_create_funnel_entry,
    )
    monkeypatch.setattr(
        sync_module,
        "enqueue_brevo_sync_job",
        fake_enqueue_brevo_sync_job,
    )

    brevo_client = DummyBrevoClient()
    connection = DummyConnection()

    service = FunnelSyncService(
        connection=connection,  # type: ignore[arg-type]
        brevo_client=brevo_client,  # type: ignore[arg-type]
        language_list_id=4,
        non_language_list_id=5,
        dry_run=True,
    )

    service.sync()

    # In dry-run mode, no Brevo calls or DB writes should occur
    assert len(brevo_client.calls) == 0
    assert len(created_entries) == 0
    assert len(enqueued_jobs) == 0
    assert connection.transactions_started == 0


def test_funnel_sync_handles_duplicate_entries_gracefully(monkeypatch):
    """Test that duplicate entries don't result in outbox jobs being enqueued."""
    language_candidates = [
        (1, "lang1@example.com"),
    ]

    created_entries = []
    enqueued_jobs = []

    def fake_get_language_test_candidates(connection, limit):
        return language_candidates

    def fake_get_non_language_test_candidates(connection, limit):
        return []

    def fake_create_funnel_entry(
        connection, email, funnel_type, user_id=None, test_id=None
    ):
        created_entries.append(
            {
                "email": email,
                "funnel_type": funnel_type,
                "user_id": user_id,
                "test_id": test_id,
            }
        )
        # Return None to simulate duplicate entry
        return None

    def fake_enqueue_brevo_sync_job(
        connection, funnel_entry_id, operation_type, payload
    ):
        enqueued_jobs.append(
            {
                "funnel_entry_id": funnel_entry_id,
                "operation_type": operation_type,
            }
        )
        raise AssertionError(
            "enqueue_brevo_sync_job must not be called for duplicate entries"
        )

    import funnels.sync_service as sync_module

    monkeypatch.setattr(
        sync_module,
        "get_language_test_candidates",
        fake_get_language_test_candidates,
    )
    monkeypatch.setattr(
        sync_module,
        "get_non_language_test_candidates",
        fake_get_non_language_test_candidates,
    )
    monkeypatch.setattr(
        sync_module,
        "create_funnel_entry",
        fake_create_funnel_entry,
    )
    monkeypatch.setattr(
        sync_module,
        "enqueue_brevo_sync_job",
        fake_enqueue_brevo_sync_job,
    )

    brevo_client = DummyBrevoClient()
    connection = DummyConnection()

    service = FunnelSyncService(
        connection=connection,  # type: ignore[arg-type]
        brevo_client=brevo_client,  # type: ignore[arg-type]
        language_list_id=4,
        non_language_list_id=5,
        dry_run=False,
    )

    service.sync()

    # Should have attempted to create entry (but got None for duplicate)
    assert len(created_entries) == 1
    # Should NOT have enqueued outbox job for duplicate
    assert len(enqueued_jobs) == 0
    # Should have committed transaction (for the duplicate check)
    assert connection.transactions_started == 1
    assert connection.commits == 1
    assert connection.rollbacks == 0
