from __future__ import annotations

from datetime import datetime, timezone


def to_utc(value: str | datetime) -> datetime:
    """Convert a string or datetime to a timezone-aware UTC datetime."""
    if isinstance(value, datetime):
        dt = value
    else:
        dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)
