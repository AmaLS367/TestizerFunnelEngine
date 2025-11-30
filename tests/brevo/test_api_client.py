import pytest
import requests

from brevo.models import BrevoContact
from brevo.api_client import BrevoApiClient, BrevoFatalError, BrevoTransientError


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
        raise AssertionError(
            "requests.request must not be called when api key is missing"
        )

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

    response = client._request(
        "POST", "/contacts", json_body={"email": "user@example.com"}
    )

    assert response == {"dry_run": True}


def test_request_raises_brevo_transient_error_on_network_exception(monkeypatch):
    """Test that network exceptions raise BrevoTransientError."""
    import brevo.api_client as api_module

    client = BrevoApiClient(
        api_key="secret-key",
        base_url="https://api.brevo.com/v3",
        dry_run=False,
    )

    def fake_request(method, url, headers=None, json=None, timeout=None):
        raise requests.ConnectionError("Connection failed")

    monkeypatch.setattr(api_module.requests, "request", fake_request)

    with pytest.raises(BrevoTransientError) as exc_info:
        client._request("POST", "/contacts", json_body={"email": "user@example.com"})

    assert "Network error" in str(exc_info.value)
    assert "Connection failed" in str(exc_info.value)


def test_request_raises_brevo_transient_error_on_429(monkeypatch):
    """Test that HTTP 429 raises BrevoTransientError."""
    import brevo.api_client as api_module

    client = BrevoApiClient(
        api_key="secret-key",
        base_url="https://api.brevo.com/v3",
        dry_run=False,
    )

    class DummyResponse:
        def __init__(self):
            self.status_code = 429
            self.text = "Rate limit exceeded"

        def json(self):
            return {}

    def fake_request(method, url, headers=None, json=None, timeout=None):
        return DummyResponse()

    monkeypatch.setattr(api_module.requests, "request", fake_request)

    with pytest.raises(BrevoTransientError) as exc_info:
        client._request("POST", "/contacts", json_body={"email": "user@example.com"})

    assert "429" in str(exc_info.value)
    assert "Rate limit exceeded" in str(exc_info.value)


def test_request_raises_brevo_transient_error_on_5xx(monkeypatch):
    """Test that HTTP 5xx raises BrevoTransientError."""
    import brevo.api_client as api_module

    client = BrevoApiClient(
        api_key="secret-key",
        base_url="https://api.brevo.com/v3",
        dry_run=False,
    )

    class DummyResponse:
        def __init__(self):
            self.status_code = 500
            self.text = "Internal server error"

        def json(self):
            return {}

    def fake_request(method, url, headers=None, json=None, timeout=None):
        return DummyResponse()

    monkeypatch.setattr(api_module.requests, "request", fake_request)

    with pytest.raises(BrevoTransientError) as exc_info:
        client._request("POST", "/contacts", json_body={"email": "user@example.com"})

    assert "500" in str(exc_info.value)
    assert "Internal server error" in str(exc_info.value)


def test_request_raises_brevo_fatal_error_on_4xx(monkeypatch):
    """Test that HTTP 4xx (except 429) raises BrevoFatalError."""
    import brevo.api_client as api_module

    client = BrevoApiClient(
        api_key="secret-key",
        base_url="https://api.brevo.com/v3",
        dry_run=False,
    )

    class DummyResponse:
        def __init__(self):
            self.status_code = 400
            self.text = "Bad request"

        def json(self):
            return {}

    def fake_request(method, url, headers=None, json=None, timeout=None):
        return DummyResponse()

    monkeypatch.setattr(api_module.requests, "request", fake_request)

    with pytest.raises(BrevoFatalError) as exc_info:
        client._request("POST", "/contacts", json_body={"email": "user@example.com"})

    assert "400" in str(exc_info.value)
    assert "Bad request" in str(exc_info.value)


def test_request_raises_brevo_fatal_error_on_404(monkeypatch):
    """Test that HTTP 404 raises BrevoFatalError."""
    import brevo.api_client as api_module

    client = BrevoApiClient(
        api_key="secret-key",
        base_url="https://api.brevo.com/v3",
        dry_run=False,
    )

    class DummyResponse:
        def __init__(self):
            self.status_code = 404
            self.text = "Not found"

        def json(self):
            return {}

    def fake_request(method, url, headers=None, json=None, timeout=None):
        return DummyResponse()

    monkeypatch.setattr(api_module.requests, "request", fake_request)

    with pytest.raises(BrevoFatalError) as exc_info:
        client._request("POST", "/contacts", json_body={"email": "user@example.com"})

    assert "404" in str(exc_info.value)
    assert "Not found" in str(exc_info.value)


def test_request_trims_long_response_body(monkeypatch):
    """Test that long response bodies are trimmed in error messages."""
    import brevo.api_client as api_module

    client = BrevoApiClient(
        api_key="secret-key",
        base_url="https://api.brevo.com/v3",
        dry_run=False,
    )

    long_text = "x" * 1000

    class DummyResponse:
        def __init__(self):
            self.status_code = 400
            self.text = long_text

        def json(self):
            return {}

    def fake_request(method, url, headers=None, json=None, timeout=None):
        return DummyResponse()

    monkeypatch.setattr(api_module.requests, "request", fake_request)

    with pytest.raises(BrevoFatalError) as exc_info:
        client._request("POST", "/contacts", json_body={"email": "user@example.com"})

    error_message = str(exc_info.value)
    assert len(error_message) < len(long_text) + 50  # Should be trimmed
    assert "..." in error_message  # Should have ellipsis


def test_create_or_update_contact_retries_on_transient_error_then_succeeds(
    monkeypatch,
):
    """Test that create_or_update_contact retries on transient error and succeeds."""
    import brevo.api_client as api_module

    attempt_count = [0]

    class DummyResponse:
        def __init__(self, status=200, text="ok"):
            self.status_code = status
            self.text = text

        def json(self):
            return {"success": True}

    def fake_request(method, url, headers=None, json=None, timeout=None):
        attempt_count[0] += 1
        if attempt_count[0] == 1:
            # First attempt fails with transient error (5xx)
            return DummyResponse(status=500, text="Internal server error")
        # Second attempt succeeds
        return DummyResponse()

    monkeypatch.setattr(api_module.requests, "request", fake_request)
    monkeypatch.setattr(api_module.time, "sleep", lambda x: None)  # No actual sleep

    client = BrevoApiClient(
        api_key="secret-key",
        base_url="https://api.brevo.com/v3",
        dry_run=False,
        max_retries=3,
        base_backoff_seconds=0.1,
    )

    contact = BrevoContact(
        email="user@example.com",
        list_ids=[1, 2],
        attributes={"FUNNEL_TYPE": "language"},
    )

    response = client.create_or_update_contact(contact)

    assert attempt_count[0] == 2  # Should have retried once
    assert response == {"success": True}


def test_create_or_update_contact_exhausts_retries_on_transient_errors(monkeypatch):
    """Test that create_or_update_contact stops after max_retries."""
    import brevo.api_client as api_module

    attempt_count = [0]

    class DummyResponse:
        def __init__(self):
            self.status_code = 500
            self.text = "Internal server error"

        def json(self):
            return {}

    def fake_request(method, url, headers=None, json=None, timeout=None):
        attempt_count[0] += 1
        # Always fail with transient error (5xx)
        return DummyResponse()

    monkeypatch.setattr(api_module.requests, "request", fake_request)
    monkeypatch.setattr(api_module.time, "sleep", lambda x: None)  # No actual sleep

    client = BrevoApiClient(
        api_key="secret-key",
        base_url="https://api.brevo.com/v3",
        dry_run=False,
        max_retries=3,
        base_backoff_seconds=0.1,
    )

    contact = BrevoContact(
        email="user@example.com",
        list_ids=[1, 2],
        attributes={"FUNNEL_TYPE": "language"},
    )

    with pytest.raises(BrevoTransientError) as exc_info:
        client.create_or_update_contact(contact)

    assert attempt_count[0] == 4  # Initial attempt + 3 retries
    assert "500" in str(exc_info.value)


def test_create_or_update_contact_retries_on_network_exception_then_succeeds(
    monkeypatch,
):
    """Test that create_or_update_contact retries on network exception and succeeds."""
    import brevo.api_client as api_module

    attempt_count = [0]

    class DummyResponse:
        def __init__(self):
            self.status_code = 200
            self.text = "ok"

        def json(self):
            return {"success": True}

    def fake_request(method, url, headers=None, json=None, timeout=None):
        attempt_count[0] += 1
        if attempt_count[0] == 1:
            # First attempt fails with network error
            raise requests.ConnectionError("Connection failed")
        # Second attempt succeeds
        return DummyResponse()

    monkeypatch.setattr(api_module.requests, "request", fake_request)
    monkeypatch.setattr(api_module.time, "sleep", lambda x: None)  # No actual sleep

    client = BrevoApiClient(
        api_key="secret-key",
        base_url="https://api.brevo.com/v3",
        dry_run=False,
        max_retries=3,
        base_backoff_seconds=0.1,
    )

    contact = BrevoContact(
        email="user@example.com",
        list_ids=[1, 2],
        attributes={"FUNNEL_TYPE": "language"},
    )

    response = client.create_or_update_contact(contact)

    assert attempt_count[0] == 2  # Should have retried once
    assert response == {"success": True}


def test_create_or_update_contact_does_not_retry_on_fatal_error(monkeypatch):
    """Test that create_or_update_contact does not retry on fatal error."""
    import brevo.api_client as api_module

    attempt_count = [0]

    class DummyResponse:
        def __init__(self):
            self.status_code = 400
            self.text = "Bad request"

        def json(self):
            return {}

    def fake_request(method, url, headers=None, json=None, timeout=None):
        attempt_count[0] += 1
        return DummyResponse()

    monkeypatch.setattr(api_module.requests, "request", fake_request)
    monkeypatch.setattr(api_module.time, "sleep", lambda x: None)  # No actual sleep

    client = BrevoApiClient(
        api_key="secret-key",
        base_url="https://api.brevo.com/v3",
        dry_run=False,
        max_retries=3,
        base_backoff_seconds=0.1,
    )

    contact = BrevoContact(
        email="user@example.com",
        list_ids=[1, 2],
        attributes={"FUNNEL_TYPE": "language"},
    )

    with pytest.raises(BrevoFatalError) as exc_info:
        client.create_or_update_contact(contact)

    assert attempt_count[0] == 1  # Should not retry
    assert "400" in str(exc_info.value)
    assert "Bad request" in str(exc_info.value)


def test_rate_limiting_sleeps_when_limit_exceeded(monkeypatch):
    """Test that rate limiting sleeps when max_requests_per_minute is exceeded."""
    import brevo.api_client as api_module

    sleep_calls = []

    class DummyResponse:
        def __init__(self):
            self.status_code = 200
            self.text = "ok"

        def json(self):
            return {"success": True}

    def fake_request(method, url, headers=None, json=None, timeout=None):
        return DummyResponse()

    def fake_sleep(duration):
        sleep_calls.append(duration)

    # Mock time.time to return controlled timestamps
    current_time = [100.0]

    def fake_time():
        return current_time[0]

    monkeypatch.setattr(api_module.requests, "request", fake_request)
    monkeypatch.setattr(api_module.time, "sleep", fake_sleep)
    monkeypatch.setattr(api_module.time, "time", fake_time)

    client = BrevoApiClient(
        api_key="secret-key",
        base_url="https://api.brevo.com/v3",
        dry_run=False,
        max_requests_per_minute=3,
    )

    contact = BrevoContact(
        email="user@example.com",
        list_ids=[1, 2],
        attributes={"FUNNEL_TYPE": "language"},
    )

    # Make 3 requests (at the limit)
    for i in range(3):
        client.create_or_update_contact(contact)
        current_time[0] += 1.0  # Advance time by 1 second

    # 4th request should trigger rate limiting
    client.create_or_update_contact(contact)

    # Should have slept once
    assert len(sleep_calls) == 1
    # Sleep duration should be approximately 60 seconds minus the time since oldest request
    assert sleep_calls[0] > 50.0  # Should sleep until oldest request is >60s old


def test_rate_limiting_does_not_sleep_when_under_limit(monkeypatch):
    """Test that rate limiting does not sleep when under the limit."""
    import brevo.api_client as api_module

    sleep_calls = []

    class DummyResponse:
        def __init__(self):
            self.status_code = 200
            self.text = "ok"

        def json(self):
            return {"success": True}

    def fake_request(method, url, headers=None, json=None, timeout=None):
        return DummyResponse()

    def fake_sleep(duration):
        sleep_calls.append(duration)

    current_time = [100.0]

    def fake_time():
        return current_time[0]

    monkeypatch.setattr(api_module.requests, "request", fake_request)
    monkeypatch.setattr(api_module.time, "sleep", fake_sleep)
    monkeypatch.setattr(api_module.time, "time", fake_time)

    client = BrevoApiClient(
        api_key="secret-key",
        base_url="https://api.brevo.com/v3",
        dry_run=False,
        max_requests_per_minute=60,
    )

    contact = BrevoContact(
        email="user@example.com",
        list_ids=[1, 2],
        attributes={"FUNNEL_TYPE": "language"},
    )

    # Make 10 requests (well under the limit)
    for i in range(10):
        client.create_or_update_contact(contact)
        current_time[0] += 1.0

    # Should not have slept
    assert len(sleep_calls) == 0


def test_circuit_breaker_opens_after_threshold_errors(monkeypatch):
    """Test that circuit breaker opens after consecutive errors exceed threshold."""
    import brevo.api_client as api_module

    class DummyResponse:
        def __init__(self, status=500, text="Error"):
            self.status_code = status
            self.text = text

        def json(self):
            return {}

    def fake_request(method, url, headers=None, json=None, timeout=None):
        return DummyResponse()

    monkeypatch.setattr(api_module.requests, "request", fake_request)
    monkeypatch.setattr(api_module.time, "sleep", lambda x: None)

    client = BrevoApiClient(
        api_key="secret-key",
        base_url="https://api.brevo.com/v3",
        dry_run=False,
        max_retries=0,  # No retries to speed up test
        circuit_error_threshold=3,
        circuit_open_seconds=60,
    )

    contact = BrevoContact(
        email="user@example.com",
        list_ids=[1, 2],
        attributes={"FUNNEL_TYPE": "language"},
    )

    # Make 3 requests that fail (threshold is 3)
    for i in range(3):
        with pytest.raises(BrevoTransientError):
            client.create_or_update_contact(contact)

    # Circuit should now be open
    assert client.circuit_open_until is not None
    assert client.consecutive_errors == 3

    # Next request should fail fast with circuit open error
    with pytest.raises(BrevoTransientError) as exc_info:
        client.create_or_update_contact(contact)

    assert "Circuit breaker is open" in str(exc_info.value)


def test_circuit_breaker_closes_after_timeout(monkeypatch):
    """Test that circuit breaker closes after timeout and allows new attempts."""
    import brevo.api_client as api_module
    from datetime import datetime, timedelta

    class DummyResponse:
        def __init__(self, status=200, text="ok"):
            self.status_code = status
            self.text = text

        def json(self):
            return {"success": True}

    class ErrorResponse:
        def __init__(self):
            self.status_code = 500
            self.text = "Internal server error"

        def json(self):
            return {}

    response_count = [0]

    def fake_request(method, url, headers=None, json=None, timeout=None):
        response_count[0] += 1
        if response_count[0] <= 3:
            # First 3 attempts fail
            return ErrorResponse()
        # 4th attempt succeeds
        return DummyResponse()

    monkeypatch.setattr(api_module.requests, "request", fake_request)
    monkeypatch.setattr(api_module.time, "sleep", lambda x: None)

    client = BrevoApiClient(
        api_key="secret-key",
        base_url="https://api.brevo.com/v3",
        dry_run=False,
        max_retries=0,
        circuit_error_threshold=3,
        circuit_open_seconds=60,
    )

    contact = BrevoContact(
        email="user@example.com",
        list_ids=[1, 2],
        attributes={"FUNNEL_TYPE": "language"},
    )

    # Make 3 requests that fail to open circuit
    for i in range(3):
        with pytest.raises(BrevoTransientError):
            client.create_or_update_contact(contact)

    # Circuit should be open
    assert client.circuit_open_until is not None

    # Manually set circuit_open_until to the past to simulate timeout
    client.circuit_open_until = datetime.now() - timedelta(seconds=1)

    # Next request should check circuit, see it's expired, reset it, and allow the request
    result = client.create_or_update_contact(contact)
    assert result == {"success": True}
    assert client.circuit_open_until is None
    assert client.consecutive_errors == 0


def test_circuit_breaker_resets_on_success(monkeypatch):
    """Test that circuit breaker error count resets on successful request."""
    import brevo.api_client as api_module

    class DummyResponse:
        def __init__(self, status=200, text="ok"):
            self.status_code = status
            self.text = text

        def json(self):
            return {"success": True}

    class ErrorResponse:
        def __init__(self):
            self.status_code = 500
            self.text = "Internal server error"

        def json(self):
            return {}

    error_count = [0]

    def fake_request(method, url, headers=None, json=None, timeout=None):
        error_count[0] += 1
        if error_count[0] <= 2:
            # First 2 requests fail
            return ErrorResponse()
        # 3rd request succeeds
        return DummyResponse()

    monkeypatch.setattr(api_module.requests, "request", fake_request)
    monkeypatch.setattr(api_module.time, "sleep", lambda x: None)

    client = BrevoApiClient(
        api_key="secret-key",
        base_url="https://api.brevo.com/v3",
        dry_run=False,
        max_retries=0,
        circuit_error_threshold=5,
    )

    contact = BrevoContact(
        email="user@example.com",
        list_ids=[1, 2],
        attributes={"FUNNEL_TYPE": "language"},
    )

    # Make 2 requests that fail
    for i in range(2):
        with pytest.raises(BrevoTransientError):
            client.create_or_update_contact(contact)

    assert client.consecutive_errors == 2

    # Next request succeeds
    client.create_or_update_contact(contact)

    # Error count should be reset
    assert client.consecutive_errors == 0
    assert client.circuit_open_until is None
