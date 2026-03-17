from __future__ import annotations

import pandas as pd

try:
    import exchange_calendars as xcals
except Exception:  # pragma: no cover
    xcals = None


def active_minutes_mask(index_utc: pd.DatetimeIndex, venue_type: str) -> pd.Series:
    if venue_type == "crypto":
        return pd.Series(True, index=index_utc)
    if venue_type in {"fx", "metal"}:
        return pd.Series(index_utc.dayofweek < 5, index=index_utc)

    if xcals is not None:
        cal = xcals.get_calendar("XNYS")
        sched = cal.schedule.loc[index_utc.min().date() : index_utc.max().date()]
        out = pd.Series(False, index=index_utc)
        for _, r in sched.iterrows():
            out |= (index_utc >= r["market_open"]) & (index_utc < r["market_close"])
        return out

    et = index_utc.tz_convert("America/New_York")
    return pd.Series((et.dayofweek < 5) & (((et.hour > 9) | ((et.hour == 9) & (et.minute >= 30))) & (et.hour < 16)), index=index_utc)
