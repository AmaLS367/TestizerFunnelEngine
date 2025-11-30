from dataclasses import dataclass
from datetime import datetime
from typing import Optional


class FunnelType:
    LANGUAGE = "language"
    NON_LANGUAGE = "non_language"


@dataclass
class FunnelCandidate:
    email: str
    funnel_type: str
    user_id: Optional[int] = None
    test_id: Optional[int] = None
    test_completed_at: Optional[datetime] = None
