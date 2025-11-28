import logging
from typing import Any, Dict, Optional

import requests

from brevo.models import BrevoContact


class BrevoApiClient:
    def __init__(self, api_key: str, base_url: str, dry_run: bool) -> None:
        self.api_key = api_key.strip()
        self.base_url = base_url.rstrip("/")
        self.dry_run = dry_run
        self.logger = logging.getLogger("brevo.api_client")

    def _build_url(self, path: str) -> str:
        if not path.startswith("/"):
            path = "/" + path
        return f"{self.base_url}{path}"

    def _request(
        self,
        method: str,
        path: str,
        json_body: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
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

        try:
            response = requests.request(
                method=method,
                url=url,
                headers=headers,
                json=json_body,
                timeout=10,
            )
        except requests.RequestException as error:
            self.logger.error("Brevo request error: %s", error)
            raise

        if response.status_code >= 400:
            self.logger.error(
                "Brevo API error %s: %s",
                response.status_code,
                response.text,
            )
            raise RuntimeError(
                f"Brevo API error {response.status_code}: {response.text}"
            )

        try:
            return response.json()
        except ValueError:
            return {}

    def create_or_update_contact(self, contact: BrevoContact) -> Dict[str, Any]:
        payload = contact.to_payload()
        self.logger.info(
            "Sending contact to Brevo (email=%s, lists=%s, dry_run=%s)",
            contact.email,
            contact.list_ids,
            self.dry_run,
        )
        return self._request("POST", "/contacts", json_body=payload)

