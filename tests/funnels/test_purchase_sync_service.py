from datetime import datetime

import pytest

from funnels import purchase_sync_service
from funnels.purchase_sync_service import PurchaseSyncService


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


class DummyBrevoClient:
    def create_or_update_contact(self, contact):
        pass


def test_purchase_sync_marks_entry_and_enqueues_outbox_job(monkeypatch):
    """Test that purchase sync updates funnel_entries and enqueues outbox job."""
    pending_entries = [
        ("user@example.com", "language", None, 42),
    ]

    calls = {"marked": [], "enqueued": []}

    def fake_get_pending_funnel_entries(connection, max_rows):
        assert max_rows == 100
        return pending_entries

    def fake_get_certificate_purchase_for_entry(
        connection, email, funnel_type, user_id, test_id
    ):
        assert email == "user@example.com"
        assert funnel_type == "language"
        assert test_id == 42
        return (123, datetime(2025, 1, 1, 12, 0, 0))

    def fake_mark_certificate_purchased(
        connection, email, funnel_type, test_id, purchased_at
    ):
        calls["marked"].append((email, funnel_type, test_id, purchased_at))

    def fake_enqueue_brevo_sync_job(
        connection, funnel_entry_id, operation_type, payload
    ):
        calls["enqueued"].append(
            {
                "funnel_entry_id": funnel_entry_id,
                "operation_type": operation_type,
                "payload": payload,
            }
        )
        return 999  # outbox job ID

    monkeypatch.setattr(
        purchase_sync_service,
        "get_pending_funnel_entries",
        fake_get_pending_funnel_entries,
    )
    monkeypatch.setattr(
        purchase_sync_service,
        "get_certificate_purchase_for_entry",
        fake_get_certificate_purchase_for_entry,
    )
    monkeypatch.setattr(
        purchase_sync_service,
        "mark_certificate_purchased",
        fake_mark_certificate_purchased,
    )
    monkeypatch.setattr(
        purchase_sync_service,
        "enqueue_brevo_sync_job",
        fake_enqueue_brevo_sync_job,
    )

    # Mock _get_funnel_entry_ids to return a funnel entry ID
    def fake_get_funnel_entry_ids(self, email, funnel_type, test_id):
        return [100]  # Return a fake funnel entry ID

    import funnels.purchase_sync_service as purchase_module
    monkeypatch.setattr(
        purchase_module.PurchaseSyncService,
        "_get_funnel_entry_ids",
        fake_get_funnel_entry_ids,
    )

    connection = DummyConnection()
    service = PurchaseSyncService(
        connection=connection,  # type: ignore[arg-type]
        brevo_client=DummyBrevoClient(),  # type: ignore[arg-type]
        dry_run=False,
    )

    service.sync(max_rows=100)

    # Should have marked entry as purchased
    assert len(calls["marked"]) == 1
    email, funnel_type, test_id, purchased_at = calls["marked"][0]
    assert email == "user@example.com"
    assert funnel_type == "language"
    assert test_id == 42
    assert isinstance(purchased_at, datetime)

    # Should have enqueued outbox job
    assert len(calls["enqueued"]) == 1
    job = calls["enqueued"][0]
    assert job["funnel_entry_id"] == 100
    assert job["operation_type"] == "update_after_purchase"

    # Should have started and committed transaction
    assert connection.transactions_started == 1
    assert connection.commits == 1
    assert connection.rollbacks == 0


def test_purchase_sync_skips_when_no_purchase_found(monkeypatch):
    pending_entries = [
        ("user@example.com", "language", None, 42),
    ]

    calls = {"marked": []}

    def fake_get_pending_funnel_entries(connection, max_rows):
        return pending_entries

    def fake_get_certificate_purchase_for_entry(
        connection, email, funnel_type, user_id, test_id
    ):
        return None

    def fake_mark_certificate_purchased(
        connection, email, funnel_type, test_id, purchased_at
    ):
        calls["marked"].append((email, funnel_type, test_id, purchased_at))

    monkeypatch.setattr(
        purchase_sync_service,
        "get_pending_funnel_entries",
        fake_get_pending_funnel_entries,
    )
    monkeypatch.setattr(
        purchase_sync_service,
        "get_certificate_purchase_for_entry",
        fake_get_certificate_purchase_for_entry,
    )
    monkeypatch.setattr(
        purchase_sync_service,
        "mark_certificate_purchased",
        fake_mark_certificate_purchased,
    )

    connection = DummyConnection()
    service = PurchaseSyncService(
        connection=connection,  # type: ignore[arg-type]
        brevo_client=DummyBrevoClient(),  # type: ignore[arg-type]
        dry_run=False,
    )
    service.sync(max_rows=100)

    assert calls["marked"] == []
    assert connection.transactions_started == 0


def test_purchase_sync_raises_value_error_for_invalid_purchase_datetime(monkeypatch):
    pending_entries = [
        ("user@example.com", "language", 10, 42),
    ]

    def fake_get_pending_funnel_entries(connection, max_rows):
        return pending_entries

    def fake_get_certificate_purchase_for_entry(
        connection, email, funnel_type, user_id, test_id
    ):
        assert email == "user@example.com"
        assert funnel_type == "language"
        assert user_id == 10
        assert test_id == 42
        return (123, "2025-01-01")

    def fake_mark_certificate_purchased(
        connection, email, funnel_type, test_id, purchased_at
    ):
        raise AssertionError(
            "mark_certificate_purchased must not be called for invalid datetime"
        )

    monkeypatch.setattr(
        purchase_sync_service,
        "get_pending_funnel_entries",
        fake_get_pending_funnel_entries,
    )
    monkeypatch.setattr(
        purchase_sync_service,
        "get_certificate_purchase_for_entry",
        fake_get_certificate_purchase_for_entry,
    )
    monkeypatch.setattr(
        purchase_sync_service,
        "mark_certificate_purchased",
        fake_mark_certificate_purchased,
    )

    connection = DummyConnection()
    service = PurchaseSyncService(
        connection=connection,  # type: ignore[arg-type]
        brevo_client=DummyBrevoClient(),  # type: ignore[arg-type]
        dry_run=False,
    )

    with pytest.raises(ValueError):
        service.sync(max_rows=100)

    # ValueError is raised before transaction starts (in _ensure_datetime),
    # so no transaction was started and no rollback is needed
    assert connection.transactions_started == 0
    assert connection.rollbacks == 0


def test_purchase_sync_dry_run_does_not_update_database_or_brevo(monkeypatch):
    """Test that dry-run mode does not call mark_certificate_purchased or enqueue outbox jobs."""
    pending_entries = [
        ("user@example.com", "language", None, 42),
    ]

    calls = {"marked": [], "enqueued": []}

    def fake_get_pending_funnel_entries(connection, max_rows):
        return pending_entries

    def fake_get_certificate_purchase_for_entry(
        connection, email, funnel_type, user_id, test_id
    ):
        return (123, datetime(2025, 1, 1, 12, 0, 0))

    def fake_mark_certificate_purchased(
        connection, email, funnel_type, test_id, purchased_at
    ):
        calls["marked"].append((email, funnel_type, test_id, purchased_at))
        raise AssertionError(
            "mark_certificate_purchased must not be called in dry-run mode"
        )

    def fake_enqueue_brevo_sync_job(
        connection, funnel_entry_id, operation_type, payload
    ):
        calls["enqueued"].append(
            {
                "funnel_entry_id": funnel_entry_id,
                "operation_type": operation_type,
            }
        )
        raise AssertionError(
            "enqueue_brevo_sync_job must not be called in dry-run mode"
        )

    monkeypatch.setattr(
        purchase_sync_service,
        "get_pending_funnel_entries",
        fake_get_pending_funnel_entries,
    )
    monkeypatch.setattr(
        purchase_sync_service,
        "get_certificate_purchase_for_entry",
        fake_get_certificate_purchase_for_entry,
    )
    monkeypatch.setattr(
        purchase_sync_service,
        "mark_certificate_purchased",
        fake_mark_certificate_purchased,
    )
    monkeypatch.setattr(
        purchase_sync_service,
        "enqueue_brevo_sync_job",
        fake_enqueue_brevo_sync_job,
    )

    connection = DummyConnection()
    service = PurchaseSyncService(
        connection=connection,  # type: ignore[arg-type]
        brevo_client=DummyBrevoClient(),  # type: ignore[arg-type]
        dry_run=True,
    )
    service.sync(max_rows=100)

    # In dry-run mode, no DB writes or outbox jobs should occur
    assert len(calls["marked"]) == 0
    assert len(calls["enqueued"]) == 0
    assert connection.transactions_started == 0
