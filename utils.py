"""
Shared helper functions for the Form 5500 ESOP Dashboard.
"""
from __future__ import annotations

import io
import logging
from datetime import datetime, timezone

import pandas as pd

logger = logging.getLogger(__name__)


def format_last_updated(iso_str: str | None) -> str:
    """Format a UTC ISO timestamp to a readable local time string."""
    if not iso_str:
        return "Never"
    try:
        dt = datetime.fromisoformat(iso_str)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.strftime("%b %d, %Y at %I:%M %p UTC")
    except (ValueError, TypeError):
        return iso_str


def to_csv_bytes(data: list[dict]) -> bytes:
    """Convert a list of dicts to CSV bytes for download."""
    if not data:
        return b""
    df = pd.DataFrame(data)
    return df.to_csv(index=False).encode("utf-8")
