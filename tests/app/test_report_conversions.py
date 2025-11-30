import sys
from datetime import datetime

import pytest

from analytics.report_service import FunnelConversion
from app import report_conversions


def test_parse_date_none_returns_none() -> None:
    assert report_conversions.parse_date(None) is None


def test_parse_date_valid_string() -> None:
    value = "2025-01-01"
    result = report_conversions.parse_date(value)

    assert isinstance(result, datetime)
    assert result.year == 2025
    assert result.month == 1
    assert result.day == 1


def test_parse_date_invalid_string_raises_value_error() -> None:
    with pytest.raises(ValueError):
        report_conversions.parse_date("invalid-date")


def test_main_prints_no_entries_message(monkeypatch, capsys) -> None:
    def fake_generate_conversion_report(from_date=None, to_date=None):
        return []

    monkeypatch.setattr(
        report_conversions,
        "generate_conversion_report",
        fake_generate_conversion_report,
    )
    monkeypatch.setattr(sys, "argv", ["report_conversions"])

    report_conversions.main()

    captured = capsys.readouterr()
    assert "No funnel entries found for the selected period." in captured.out


def test_main_prints_conversion_report(monkeypatch, capsys) -> None:
    items = [
        FunnelConversion(funnel_type="language", total_entries=10, total_purchased=4),
        FunnelConversion(
            funnel_type="non_language", total_entries=5, total_purchased=1
        ),
    ]

    def fake_generate_conversion_report(from_date=None, to_date=None):
        return items

    monkeypatch.setattr(
        report_conversions,
        "generate_conversion_report",
        fake_generate_conversion_report,
    )
    monkeypatch.setattr(
        sys,
        "argv",
        ["report_conversions", "--from-date", "2024-01-01", "--to-date", "2024-02-01"],
    )

    report_conversions.main()

    captured = capsys.readouterr()
    assert "Funnel conversion report" in captured.out
    assert "language: entries=10, purchased=4, conversion=40.00%" in captured.out
    assert "non_language: entries=5, purchased=1, conversion=20.00%" in captured.out
