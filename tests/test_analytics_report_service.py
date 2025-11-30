from datetime import date, datetime
from unittest.mock import MagicMock
from contextlib import contextmanager

from analytics import report_service
from analytics.report_service import generate_conversion_report, FunnelConversion
from db import connection
from config import settings


class DummyConnection:
    pass


def test_generate_conversion_report_builds_funnel_conversions(monkeypatch):
    dummy_rows = [
        ("language", 10, 3),
        ("non_language", 5, 0),
    ]

    dummy_conn = DummyConnection()

    def fake_get_funnel_conversion_summary(connection, from_date, to_date):
        assert isinstance(connection, DummyConnection)
        assert from_date == datetime(2024, 1, 1)
        assert to_date == datetime(2024, 12, 31)
        return dummy_rows

    @contextmanager
    def fake_database_connection_scope(database_settings):
        yield dummy_conn

    def fake_load_settings():
        mock_settings = MagicMock()
        mock_settings.database = MagicMock()
        return mock_settings

    monkeypatch.setattr(
        report_service,
        "get_funnel_conversion_summary",
        fake_get_funnel_conversion_summary,
    )
    monkeypatch.setattr(
        report_service,
        "database_connection_scope",
        fake_database_connection_scope,
    )
    monkeypatch.setattr(
        report_service,
        "load_settings",
        fake_load_settings,
    )

    report = generate_conversion_report(
        from_date=datetime(2024, 1, 1),
        to_date=datetime(2024, 12, 31),
    )

    assert isinstance(report, list)
    assert len(report) == 2

    language_item = next(
        item for item in report if item.funnel_type == "language"
    )
    assert isinstance(language_item, FunnelConversion)
    assert language_item.total_entries == 10
    assert language_item.total_purchased == 3
    assert language_item.conversion_rate == 0.3

    non_language_item = next(
        item for item in report if item.funnel_type == "non_language"
    )
    assert isinstance(non_language_item, FunnelConversion)
    assert non_language_item.total_entries == 5
    assert non_language_item.total_purchased == 0
    assert non_language_item.conversion_rate == 0.0

