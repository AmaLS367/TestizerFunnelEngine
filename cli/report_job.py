import argparse
import logging
from datetime import datetime, timedelta
from typing import Optional

from config.settings import load_settings
from db.connection import database_connection_scope
from logging_config.logger import configure_logging
from analytics.reports import get_funnel_conversion_report
from funnels.models import FunnelType


def parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Funnel conversion report for Testizer email funnels",
    )

    parser.add_argument(
        "--funnel",
        choices=[FunnelType.LANGUAGE, FunnelType.NON_LANGUAGE],
        required=True,
        help="Funnel type to build report for",
    )

    parser.add_argument(
        "--days",
        type=int,
        default=30,
        help="Number of days back from now to include in the report (default: 30)",
    )

    return parser.parse_args()


def build_period(days: int) -> tuple[Optional[datetime], Optional[datetime]]:
    if days <= 0:
        return None, None

    period_end = datetime.utcnow()
    period_start = period_end - timedelta(days=days)

    return period_start, period_end


def main() -> None:
    args = parse_arguments()
    settings = load_settings()
    configure_logging(settings.application.log_level)

    logger = logging.getLogger("cli.report_job")

    logger.info(
        "Building conversion report for funnel=%s, days=%s", args.funnel, args.days
    )

    period_start, period_end = build_period(args.days)

    with database_connection_scope(settings.database) as connection:
        report = get_funnel_conversion_report(
            connection=connection,
            funnel_type=args.funnel,
            period_start=period_start,
            period_end=period_end,
        )

    print("")
    print("Funnel conversion report")
    print("------------------------")
    print(f"Funnel type:          {report.funnel_type}")

    if period_start is not None and period_end is not None:
        print(f"Period start (UTC):   {period_start.isoformat()}")
        print(f"Period end   (UTC):   {period_end.isoformat()}")
    else:
        print("Period:               all time")

    print(f"Total entries:        {report.total_entries}")
    print(f"Total purchases:      {report.total_purchases}")
    print(f"Conversion rate:      {report.conversion_rate:.2f}%")
    print("")


if __name__ == "__main__":
    main()
