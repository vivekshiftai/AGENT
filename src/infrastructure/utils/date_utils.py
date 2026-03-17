"""Date utilities for planning and Gantt."""
from datetime import date, datetime
from typing import Optional, Union


def parse_date(value: Optional[Union[str, date, datetime]]) -> Optional[date]:
    """Parse string or date/datetime to date."""
    if value is None:
        return None
    if isinstance(value, date) and not isinstance(value, datetime):
        return value
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, str):
        value = value.strip()[:10]
        if len(value) == 10:
            return date.fromisoformat(value)
    return None


def format_date_for_gantt(d: date) -> str:
    """Format date for Frappe Gantt (YYYY-MM-DD)."""
    return d.isoformat()
