from __future__ import annotations

import pandas as pd


def session_bucket(ts_utc: pd.Timestamp, venue_type: str) -> str:
    if venue_type == "equity_us":
        et = ts_utc.tz_convert("America/New_York")
        if et.hour < 11: return "US_OPEN"
        if et.hour < 14: return "US_MID"
        return "US_CLOSE"
    h = ts_utc.hour
    if venue_type in {"fx", "metal"}:
        if 0 <= h < 8: return "TOKYO"
        if 8 <= h < 13: return "LONDON"
        if 13 <= h < 21: return "NY"
        return "OFF"
    return f"UTC_{h:02d}"
