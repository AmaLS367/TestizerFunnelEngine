import pytest

from brevo.models import BrevoContact
from brevo.api_client import BrevoApiClient


def test_create_or_update_contact_sends_correct_request(monkeypatch):
    calls = {}

    import brevo.api_client as api_module

    def fake_request(method, url, headers=None, json=None, timeout=None):
        calls["method"] = method
        calls["url"] = url
        calls["headers"] = headers
        calls["json"] = json

        class DummyResponse:
            def __init__(self):
                self.status_code = 200
                self.text = "ok"

            def json(self):
                return {"success": True}

        return DummyResponse()

    monkeypatch.setattr(api_module.requests, "request", fake_request)

    client = BrevoApiClient(
        api_key="secret-key",
        base_url="https://api.brevo.com/v3",
        dry_run=False,
    )

    contact = BrevoContact(
        email="user@example.com",
        list_ids=[1, 2],
        attributes={"FUNNEL_TYPE": "language"},
    )

    response = client.create_or_update_contact(contact)

    assert calls["method"] == "POST"
    assert calls["url"].endswith("/contacts")
    assert "api-key" in calls["headers"]
    assert calls["headers"]["api-key"] == "secret-key"
    assert calls["json"]["email"] == "user@example.com"
    assert calls["json"]["listIds"] == [1, 2]
    assert calls["json"]["attributes"]["FUNNEL_TYPE"] == "language"
    assert response == {"success": True}


def test_request_raises_runtime_error_when_api_key_missing(monkeypatch):
    import brevo.api_client as api_module

    client = BrevoApiClient(
        api_key="",
        base_url="https://api.brevo.com/v3",
        dry_run=False,
    )

    def fake_request(method, url, headers=None, json=None, timeout=None):
        raise AssertionError("requests.request must not be called when api key is missing")

    monkeypatch.setattr(api_module.requests, "request", fake_request)

    with pytest.raises(RuntimeError):
        client._request("POST", "/contacts", json_body={"email": "user@example.com"})


def test_request_in_dry_run_mode_does_not_call_requests(monkeypatch):
    import brevo.api_client as api_module

    client = BrevoApiClient(
        api_key="",
        base_url="https://api.brevo.com/v3",
        dry_run=True,
    )

    def fake_request(method, url, headers=None, json=None, timeout=None):
        raise AssertionError("requests.request must not be called in dry_run mode")

    monkeypatch.setattr(api_module.requests, "request", fake_request)

    response = client._request("POST", "/contacts", json_body={"email": "user@example.com"})

    assert response == {"dry_run": True}

