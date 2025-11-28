from dataclasses import dataclass, field
from typing import Any, Dict, List


@dataclass
class BrevoContact:
    email: str
    list_ids: List[int] = field(default_factory=list)
    attributes: Dict[str, Any] = field(default_factory=dict)
    update_enabled: bool = True

    def to_payload(self) -> Dict[str, Any]:
        payload: Dict[str, Any] = {
            "email": self.email,
            "updateEnabled": self.update_enabled,
        }

        if self.list_ids:
            payload["listIds"] = self.list_ids
        if self.attributes:
            payload["attributes"] = self.attributes

        return payload

