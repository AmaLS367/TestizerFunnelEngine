from funnels.sync_service import FunnelSyncService
from brevo.models import BrevoContact


class DummyBrevoClient:
    def __init__(self):
        self.calls = []

    def create_or_update_contact(self, contact: BrevoContact):
        self.calls.append(contact)


def test_funnel_sync_sends_candidates_and_creates_entries(monkeypatch):
    language_candidates = [
        (1, "lang1@example.com"),
        (2, "lang2@example.com"),
    ]

    non_language_candidates = [
        (3, "non1@example.com"),
    ]

    created_entries = []

    def fake_get_language_test_candidates(connection, limit):
        return language_candidates

    def fake_get_non_language_test_candidates(connection, limit):
        return non_language_candidates

    def fake_funnel_entry_exists(connection, email, funnel_type, test_id=None):
        return False

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
        "funnel_entry_exists",
        fake_funnel_entry_exists,
    )
    monkeypatch.setattr(
        sync_module,
        "create_funnel_entry",
        fake_create_funnel_entry,
    )

    brevo_client = DummyBrevoClient()

    service = FunnelSyncService(
        connection=object(),  # type: ignore[arg-type]
        brevo_client=brevo_client,  # type: ignore[arg-type]
        language_list_id=4,
        non_language_list_id=5,
    )

    service.sync()

    assert len(brevo_client.calls) == 3

    emails = [contact.email for contact in brevo_client.calls]
    assert set(emails) == {
        "lang1@example.com",
        "lang2@example.com",
        "non1@example.com",
    }

    assert len(created_entries) == 3


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

    service = FunnelSyncService(
        connection=object(),  # type: ignore[arg-type]
        brevo_client=brevo_client,  # type: ignore[arg-type]
        language_list_id=4,
        non_language_list_id=5,
    )

    service.sync()

    assert len(brevo_client.calls) == 0
