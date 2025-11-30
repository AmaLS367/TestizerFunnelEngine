from dataclasses import dataclass, field
from typing import Any, Dict, List


@dataclass
class BrevoContact:
    """Data model for Brevo contact creation/update operations.

    Represents a contact that will be sent to Brevo API. The to_payload method
    handles conditional field inclusion to match Brevo's API requirements.
    """

    email: str
    list_ids: List[int] = field(default_factory=list)
    attributes: Dict[str, Any] = field(default_factory=dict)
    update_enabled: bool = True

    def to_payload(self) -> Dict[str, Any]:
        """Converts contact to Brevo API payload format.

        Only includes listIds and attributes if they are non-empty, following
        Brevo API best practices. This prevents sending empty arrays/objects
        that might cause API validation errors.

        Returns:
            Dictionary ready for JSON serialization to Brevo API.
        """
        payload: Dict[str, Any] = {
            "email": self.email,
            "updateEnabled": self.update_enabled,
        }

        if self.list_ids:
            payload["listIds"] = self.list_ids
        if self.attributes:
            payload["attributes"] = self.attributes

        return payload
