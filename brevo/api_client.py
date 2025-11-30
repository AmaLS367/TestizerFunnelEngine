import logging
import random
import time
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

import requests

from brevo.models import BrevoContact


class BrevoClientError(Exception):
    """Base exception for all Brevo client errors."""


class BrevoTransientError(BrevoClientError):
    """Error that is safe to retry later.

    This includes network issues, rate limiting (429), and server errors (5xx).
    """


class BrevoFatalError(BrevoClientError):
    """Error that should not be retried.

    This includes most 4xx client errors (except 429 which is transient).
    """


class BrevoApiClient:
    """HTTP client for Brevo (formerly Sendinblue) email marketing API.

    Handles all external API communication with Brevo, including contact management.
    In dry-run mode, all requests are logged but not executed, allowing safe testing
    without side effects.
    """

    def __init__(
        self,
        api_key: str,
        base_url: str,
        dry_run: bool,
        max_retries: int = 3,
        base_backoff_seconds: float = 1.0,
        max_requests_per_minute: int = 60,
        circuit_open_seconds: int = 60,
        circuit_error_threshold: int = 5,
    ) -> None:
        """Initializes the Brevo API client.

        Args:
            api_key: Brevo API key from account settings. Must be non-empty
                unless dry_run is True.
            base_url: Base URL for Brevo API (typically https://api.brevo.com/v3).
                Trailing slashes are automatically stripped.
            dry_run: If True, all API calls are logged but not executed.
                Used for testing without creating real contacts or consuming API quota.
            max_retries: Maximum number of retry attempts for transient errors.
                Defaults to 3.
            base_backoff_seconds: Base delay in seconds for exponential backoff.
                Defaults to 1.0.
            max_requests_per_minute: Maximum number of requests allowed per minute.
                Defaults to 60.
            circuit_open_seconds: Number of seconds to keep circuit breaker open
                after threshold is reached. Defaults to 60.
            circuit_error_threshold: Number of consecutive errors before opening
                circuit breaker. Defaults to 5.
        """
        self.api_key = api_key.strip()
        self.base_url = base_url.rstrip("/")
        self.dry_run = dry_run
        self.max_retries = max_retries
        self.base_backoff_seconds = base_backoff_seconds
        self.max_requests_per_minute = max_requests_per_minute
        self.circuit_open_seconds = circuit_open_seconds
        self.circuit_error_threshold = circuit_error_threshold
        self.logger = logging.getLogger("brevo.api_client")

        # Rate limiting state
        self._request_timestamps: List[float] = []

        # Circuit breaker state
        self.consecutive_errors: int = 0
        self.circuit_open_until: Optional[datetime] = None

    def _build_url(self, path: str) -> str:
        if not path.startswith("/"):
            path = "/" + path
        return f"{self.base_url}{path}"

    def _check_circuit_breaker(self) -> None:
        """Checks circuit breaker state and raises BrevoTransientError if circuit is open.

        Raises:
            BrevoTransientError: If circuit breaker is open and timeout hasn't passed.
        """
        now = datetime.now()
        if self.circuit_open_until is not None:
            if now < self.circuit_open_until:
                self.logger.warning(
                    "Circuit breaker is open until %s. Request blocked.",
                    self.circuit_open_until,
                )
                raise BrevoTransientError(
                    f"Circuit breaker is open. Retry after {self.circuit_open_until}"
                )
            else:
                # Circuit timeout has passed, reset it
                self.logger.info("Circuit breaker timeout expired. Resetting circuit.")
                self.circuit_open_until = None
                self.consecutive_errors = 0

    def _apply_rate_limiting(self) -> None:
        """Applies rate limiting by sleeping if necessary.

        Removes timestamps older than 60 seconds and sleeps if the number
        of recent requests exceeds max_requests_per_minute.
        """
        now = time.time()
        cutoff_time = now - 60.0

        # Remove timestamps older than 60 seconds
        self._request_timestamps = [
            ts for ts in self._request_timestamps if ts > cutoff_time
        ]

        # Check if we're at the rate limit
        if len(self._request_timestamps) >= self.max_requests_per_minute:
            # Calculate how long to sleep until the oldest timestamp moves out
            oldest_timestamp = min(self._request_timestamps)
            sleep_duration = (oldest_timestamp + 60.0) - now

            if sleep_duration > 0:
                self.logger.info(
                    "Rate limit reached (%d requests in last 60s). "
                    "Sleeping for %.2f seconds",
                    len(self._request_timestamps),
                    sleep_duration,
                )
                time.sleep(sleep_duration)

                # Clean up again after sleep
                now = time.time()
                cutoff_time = now - 60.0
                self._request_timestamps = [
                    ts for ts in self._request_timestamps if ts > cutoff_time
                ]

    def _record_request_attempt(self) -> None:
        """Records a request attempt timestamp for rate limiting."""
        self._request_timestamps.append(time.time())

    def _handle_request_success(self) -> None:
        """Handles successful request by resetting circuit breaker error count."""
        self.consecutive_errors = 0

    def _handle_request_error(self) -> None:
        """Handles request error by incrementing error count and opening circuit if needed."""
        self.consecutive_errors += 1
        if self.consecutive_errors >= self.circuit_error_threshold:
            self.circuit_open_until = datetime.now() + timedelta(
                seconds=self.circuit_open_seconds
            )
            self.logger.error(
                "Circuit breaker opened after %d consecutive errors. "
                "Will remain open until %s",
                self.consecutive_errors,
                self.circuit_open_until,
            )

    def _request(
        self,
        method: str,
        path: str,
        json_body: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """Performs HTTP request to Brevo API or logs it in dry-run mode.

        Side Effects:
            - In production mode: Makes actual HTTP request to Brevo API.
            - In dry-run mode: Only logs the request without executing it.

        Edge cases:
            - Empty API key raises RuntimeError (except in dry-run mode).
            - HTTP errors are classified as transient (429, 5xx) or fatal (other 4xx).
            - Network failures raise BrevoTransientError.
            - Invalid JSON responses return empty dict instead of crashing.

        Args:
            method: HTTP method (GET, POST, PUT, DELETE).
            path: API endpoint path (e.g., "/contacts"). Leading slash is added
                automatically if missing.
            json_body: Optional JSON payload for request body.

        Returns:
            Response JSON as dictionary. Empty dict if response is not valid JSON.
            In dry-run mode, returns {"dry_run": True}.

        Raises:
            RuntimeError: If API key is missing (non-dry-run).
            BrevoTransientError: If network request fails or API returns 429/5xx.
            BrevoFatalError: If API returns 4xx (except 429).
        """
        url = self._build_url(path)
        if self.dry_run:
            self.logger.info(
                "Brevo dry run request: %s %s payload=%s",
                method,
                url,
                json_body,
            )
            return {"dry_run": True}

        if not self.api_key:
            raise RuntimeError("Brevo API key is not configured")

        headers = {
            "api-key": self.api_key,
            "Content-Type": "application/json",
            "Accept": "application/json",
        }

        # Apply rate limiting before the actual request
        self._apply_rate_limiting()

        try:
            response = requests.request(
                method=method,
                url=url,
                headers=headers,
                json=json_body,
                timeout=10,
            )
            # Record the request attempt (successful HTTP call)
            self._record_request_attempt()
        except requests.RequestException as error:
            self.logger.error("Brevo request error: %s", error)
            # Record the request attempt (failed HTTP call)
            self._record_request_attempt()
            raise BrevoTransientError(f"Network error: {error}") from error

        if response.status_code >= 400:
            # Trim response body for error messages (limit to 500 chars)
            original_text = response.text if response.text else ""
            if len(original_text) > 500:
                response_text = original_text[:500] + "..."
            else:
                response_text = original_text

            if response.status_code == 429 or response.status_code >= 500:
                self.logger.error(
                    "Brevo API transient error %s: %s",
                    response.status_code,
                    response_text,
                )
                raise BrevoTransientError(
                    f"Brevo API error {response.status_code}: {response_text}"
                )
            else:
                # Other 4xx errors are fatal
                self.logger.error(
                    "Brevo API fatal error %s: %s",
                    response.status_code,
                    response_text,
                )
                raise BrevoFatalError(
                    f"Brevo API error {response.status_code}: {response_text}"
                )

        try:
            return response.json()  # type: ignore[no-any-return]
        except ValueError:
            return {}  # type: ignore[return-value]

    def create_or_update_contact(self, contact: BrevoContact) -> Dict[str, Any]:
        """Creates or updates a contact in Brevo (upsert operation).

        This is an upsert: if contact with the email exists, it's updated;
        otherwise, a new contact is created. The operation is idempotent:
        calling multiple times with the same email produces the same result.

        Side Effects:
            - Creates or updates contact in Brevo (unless dry-run mode).
            - Adds contact to specified lists if list_ids are provided.
            - Updates contact attributes if provided.

        Args:
            contact: BrevoContact object containing email, list IDs, and attributes.

        Returns:
            API response dictionary. In dry-run mode, returns {"dry_run": True}.

        Raises:
            BrevoTransientError: If network error or API returns 429/5xx after
                all retry attempts are exhausted.
            BrevoFatalError: If API returns 4xx (except 429). Fatal errors are
                not retried.
        """
        payload = contact.to_payload()
        self.logger.info(
            "Sending contact to Brevo (email=%s, lists=%s, dry_run=%s)",
            contact.email,
            contact.list_ids,
            self.dry_run,
        )

        # Check circuit breaker before starting retry loop
        self._check_circuit_breaker()

        last_error: Optional[BrevoTransientError] = None
        for attempt in range(self.max_retries + 1):
            try:
                result = self._request("POST", "/contacts", json_body=payload)
                # Success - reset circuit breaker error count
                self._handle_request_success()
                return result
            except BrevoFatalError:
                # Fatal errors should not be retried
                # Record error for circuit breaker
                self._handle_request_error()
                raise
            except BrevoTransientError as error:
                last_error = error
                # Record error for circuit breaker
                self._handle_request_error()

                if attempt < self.max_retries:
                    # Calculate exponential backoff with jitter
                    sleep_seconds = self.base_backoff_seconds * (2**attempt)
                    jitter = random.uniform(0, 0.5)
                    total_sleep = sleep_seconds + jitter

                    self.logger.warning(
                        "Transient error on attempt %d/%d: %s. Retrying in %.2f seconds",
                        attempt + 1,
                        self.max_retries + 1,
                        str(error),
                        total_sleep,
                    )
                    time.sleep(total_sleep)
                else:
                    # All retries exhausted
                    self.logger.error(
                        "All %d retry attempts exhausted for contact %s",
                        self.max_retries + 1,
                        contact.email,
                    )
                    raise

        # This should never be reached, but mypy needs it
        assert last_error is not None
        raise last_error
