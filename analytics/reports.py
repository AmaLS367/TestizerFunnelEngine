from dataclasses import dataclass
from datetime import datetime
from typing import Any, List, Optional

from mysql.connector import MySQLConnection


@dataclass
class FunnelConversionReport:
    """Aggregated conversion statistics for a single funnel type.

    Used for reporting and analytics. The conversion_rate property handles
    zero-division edge cases to prevent runtime crashes during empty reporting
    periods.
    """

    funnel_type: str
    period_start: Optional[datetime]
    period_end: Optional[datetime]
    total_entries: int
    total_purchases: int

    @property
    def conversion_rate(self) -> float:
        """Calculates conversion rate as percentage.

        Edge case handling: Returns 0.0 when total_entries is zero to prevent
        ZeroDivisionError during empty reporting periods. This ensures reports
        can be generated even when no funnel activity exists.

        Returns:
            Conversion rate as percentage (0.0 to 100.0). Returns 0.0 if no
            entries exist to avoid division by zero.
        """
        if self.total_entries == 0:
            return 0.0
        return (self.total_purchases / self.total_entries) * 100.0


def get_funnel_conversion_report(
    connection: MySQLConnection,
    funnel_type: str,
    period_start: Optional[datetime] = None,
    period_end: Optional[datetime] = None,
) -> FunnelConversionReport:
    """Builds a conversion report for a specific funnel type and optional period.

    Aggregates total entries and purchases from funnel_entries table, applying
    date filters if provided. Period is applied to entered_at field, meaning
    it filters when users entered the funnel, not when they purchased.

    Args:
        connection: Active MySQL database connection.
        funnel_type: Either 'language' or 'non_language'.
        period_start: Start of reporting period (inclusive). None means no
            lower bound (all historical data).
        period_end: End of reporting period (exclusive). None means no upper
            bound (up to current time).

    Returns:
        FunnelConversionReport with aggregated statistics. If no entries exist,
        returns report with zero values (conversion_rate will be 0.0).
    """
    total_entries = _count_funnel_entries(
        connection=connection,
        funnel_type=funnel_type,
        period_start=period_start,
        period_end=period_end,
    )

    total_purchases = _count_funnel_purchases(
        connection=connection,
        funnel_type=funnel_type,
        period_start=period_start,
        period_end=period_end,
    )

    report = FunnelConversionReport(
        funnel_type=funnel_type,
        period_start=period_start,
        period_end=period_end,
        total_entries=total_entries,
        total_purchases=total_purchases,
    )

    return report


def _count_funnel_entries(
    connection: MySQLConnection,
    funnel_type: str,
    period_start: Optional[datetime],
    period_end: Optional[datetime],
) -> int:
    cursor = connection.cursor()

    base_query = """
    SELECT COUNT(*)
    FROM funnel_entries
    WHERE funnel_type = %s
    """

    params: List[Any] = [funnel_type]

    if period_start is not None:
        base_query += " AND entered_at >= %s"
        params.append(period_start)  # type: ignore[arg-type]

    if period_end is not None:
        base_query += " AND entered_at < %s"
        params.append(period_end)  # type: ignore[arg-type]

    cursor.execute(base_query, tuple(params))
    row = cursor.fetchone()
    cursor.close()

    return int(row[0]) if row is not None and row[0] is not None else 0  # type: ignore[arg-type]


def _count_funnel_purchases(
    connection: MySQLConnection,
    funnel_type: str,
    period_start: Optional[datetime],
    period_end: Optional[datetime],
) -> int:
    cursor = connection.cursor()

    base_query = """
    SELECT COUNT(*)
    FROM funnel_entries
    WHERE funnel_type = %s
      AND certificate_purchased = 1
    """

    params: List[Any] = [funnel_type]

    if period_start is not None:
        base_query += " AND entered_at >= %s"
        params.append(period_start)  # type: ignore[arg-type]

    if period_end is not None:
        base_query += " AND entered_at < %s"
        params.append(period_end)  # type: ignore[arg-type]

    cursor.execute(base_query, tuple(params))
    row = cursor.fetchone()
    cursor.close()

    return int(row[0]) if row is not None else 0  # type: ignore[arg-type]
