from dataclasses import dataclass
from datetime import datetime
from typing import List, Optional

from config.settings import load_settings
from db.connection import database_connection_scope
from db.selectors import get_funnel_conversion_summary


@dataclass
class FunnelConversion:
    """Conversion statistics for a single funnel type.

    Used by CLI reporting tools. The conversion_rate property handles zero-division
    to prevent runtime crashes during empty reporting periods.
    """

    funnel_type: str
    total_entries: int
    total_purchased: int

    @property
    def conversion_rate(self) -> float:
        """Calculates conversion rate as decimal (0.0 to 1.0).

        Edge case handling: Returns 0.0 when total_entries is zero to prevent
        ZeroDivisionError. This ensures reports can be generated even when no
        funnel activity exists.

        Returns:
            Conversion rate as decimal fraction. Returns 0.0 if no entries exist.
        """
        if self.total_entries == 0:
            return 0.0
        return self.total_purchased / self.total_entries


def generate_conversion_report(
    from_date: Optional[datetime],
    to_date: Optional[datetime],
) -> List[FunnelConversion]:
    """Generates conversion report for all funnel types within optional date range.

    Loads database settings, establishes connection, and aggregates statistics
    from funnel_entries table. Returns results for all funnel types found in
    the database.

    Edge case: Returns empty list if no funnel entries exist, allowing callers
    to handle zero-state gracefully.

    Args:
        from_date: Start of reporting period (inclusive). None means no lower bound.
        to_date: End of reporting period (exclusive). None means no upper bound.

    Returns:
        List of FunnelConversion objects, one per funnel type. Empty list if
        no entries exist in the specified period.
    """
    settings = load_settings()

    with database_connection_scope(settings.database) as connection:
        rows = get_funnel_conversion_summary(
            connection=connection,
            from_date=from_date,
            to_date=to_date,
        )

    report: List[FunnelConversion] = []

    for funnel_type, total_entries, total_purchased in rows:
        report.append(
            FunnelConversion(
                funnel_type=funnel_type,
                total_entries=total_entries,
                total_purchased=total_purchased,
            )
        )

    return report
