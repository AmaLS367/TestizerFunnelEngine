from datetime import datetime

from funnels.models import FunnelCandidate, FunnelType


def test_funnel_type_constants() -> None:
    assert FunnelType.LANGUAGE == "language"
    assert FunnelType.NON_LANGUAGE == "non_language"


def test_funnel_candidate_defaults() -> None:
    candidate = FunnelCandidate(
        email="user@example.com",
        funnel_type=FunnelType.LANGUAGE,
    )

    assert candidate.email == "user@example.com"
    assert candidate.funnel_type == FunnelType.LANGUAGE
    assert candidate.user_id is None
    assert candidate.test_id is None
    assert candidate.test_completed_at is None


def test_funnel_candidate_with_all_fields() -> None:
    completed_at = datetime(2025, 1, 1, 12, 0, 0)

    candidate = FunnelCandidate(
        email="user@example.com",
        funnel_type=FunnelType.NON_LANGUAGE,
        user_id=10,
        test_id=20,
        test_completed_at=completed_at,
    )

    assert candidate.email == "user@example.com"
    assert candidate.funnel_type == FunnelType.NON_LANGUAGE
    assert candidate.user_id == 10
    assert candidate.test_id == 20
    assert candidate.test_completed_at == completed_at
