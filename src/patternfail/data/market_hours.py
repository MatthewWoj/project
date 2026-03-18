from __future__ import annotations

import pandas as pd

try:
    import exchange_calendars as xcals
except Exception:  # pragma: no cover
    xcals = None


def _schedule_open_close_cols(sched: pd.DataFrame) -> tuple[str, str]:
    open_candidates = ["market_open", "open"]
    close_candidates = ["market_close", "close"]
    open_col = next((c for c in open_candidates if c in sched.columns), None)
    close_col = next((c for c in close_candidates if c in sched.columns), None)
    if open_col is None or close_col is None:
        raise KeyError(f"Schedule is missing open/close columns. Found: {list(sched.columns)}")
    return open_col, close_col


def active_minutes_mask(index_utc: pd.DatetimeIndex, venue_type: str) -> pd.Series:
    if venue_type == "crypto":
        return pd.Series(True, index=index_utc)
    if venue_type in {"fx", "metal"}:
        return pd.Series(index_utc.dayofweek < 5, index=index_utc)

    if xcals is not None:
        cal = xcals.get_calendar("XNYS")
        sched = cal.schedule.loc[index_utc.min().date() : index_utc.max().date()]
        open_col, close_col = _schedule_open_close_cols(sched)
        out = pd.Series(False, index=index_utc)
        for _, row in sched.iterrows():
            out |= (index_utc >= row[open_col]) & (index_utc < row[close_col])
        return out

    et = index_utc.tz_convert("America/New_York")
    return pd.Series((et.dayofweek < 5) & (((et.hour > 9) | ((et.hour == 9) & (et.minute >= 30))) & (et.hour < 16)), index=index_utc)
