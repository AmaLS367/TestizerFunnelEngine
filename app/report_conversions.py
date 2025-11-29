from __future__ import annotations

from argparse import ArgumentParser
from datetime import datetime
from typing import Optional

from analytics.report_service import generate_conversion_report


def parse_date(value: Optional[str]) -> Optional[datetime]:
    if value is None:
        return None

    return datetime.strptime(value, "%Y-%m-%d")


def main() -> None:
    parser = ArgumentParser(
        description="Conversion report for language and non language funnels",
    )

    parser.add_argument(
        "--from-date",
        dest="from_date",
        required=False,
        help="Start date in format YYYY-MM-DD (inclusive)",
    )

    parser.add_argument(
        "--to-date",
        dest="to_date",
        required=False,
        help="End date in format YYYY-MM-DD (exclusive)",
    )

    args = parser.parse_args()

    from_date = parse_date(args.from_date)
    to_date = parse_date(args.to_date)

    report = generate_conversion_report(
        from_date=from_date,
        to_date=to_date,
    )

    if not report:
        print("No funnel entries found for the selected period.")
        return

    print("Funnel conversion report")
    print("------------------------")

    for item in report:
        rate = 0.0

        if item.total_entries > 0:
            rate = item.total_purchased / item.total_entries * 100.0

        print(
            f"{item.funnel_type}: "
            f"entries={item.total_entries}, "
            f"purchased={item.total_purchased}, "
            f"conversion={rate:.2f}%"
        )


if __name__ == "__main__":
    main()

