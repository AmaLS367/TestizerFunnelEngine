from dataclasses import dataclass
from datetime import datetime
from typing import List, Optional

from config.settings import load_settings
from db.connection import database_connection_scope
from db.selectors import get_funnel_conversion_summary


@dataclass
class FunnelConversion:
    funnel_type: str
    total_entries: int
    total_purchased: int

    @property
    def conversion_rate(self) -> float:
        if self.total_entries == 0:
            return 0.0
        return self.total_purchased / self.total_entries


def generate_conversion_report(
    from_date: Optional[datetime],
    to_date: Optional[datetime],
) -> List[FunnelConversion]:
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

