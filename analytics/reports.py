from dataclasses import dataclass
from datetime import datetime
from typing import Any, List, Optional

from mysql.connector import MySQLConnection


@dataclass
class FunnelConversionReport:
    funnel_type: str
    period_start: Optional[datetime]
    period_end: Optional[datetime]
    total_entries: int
    total_purchases: int

    @property
    def conversion_rate(self) -> float:
        if self.total_entries == 0:
            return 0.0
        return (self.total_purchases / self.total_entries) * 100.0


def get_funnel_conversion_report(
    connection: MySQLConnection,
    funnel_type: str,
    period_start: Optional[datetime] = None,
    period_end: Optional[datetime] = None,
) -> FunnelConversionReport:
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
        params.append(period_start)

    if period_end is not None:
        base_query += " AND entered_at < %s"
        params.append(period_end)

    cursor.execute(base_query, tuple(params))
    row = cursor.fetchone()
    cursor.close()

    return int(row[0]) if row is not None and row[0] is not None else 0


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

    params = [funnel_type]

    if period_start is not None:
        base_query += " AND entered_at >= %s"
        params.append(period_start)

    if period_end is not None:
        base_query += " AND entered_at < %s"
        params.append(period_end)

    cursor.execute(base_query, tuple(params))
    row = cursor.fetchone()
    cursor.close()

    return int(row[0]) if row is not None else 0

