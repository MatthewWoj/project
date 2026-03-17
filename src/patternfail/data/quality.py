from __future__ import annotations

import pandas as pd

from .market_hours import active_minutes_mask


def missing_bar_report(df: pd.DataFrame, venue_type: str) -> pd.DataFrame:
    idx = pd.DatetimeIndex(df["ts_utc"])
    full_idx = pd.date_range(idx.min(), idx.max(), freq="min", tz="UTC")
    active = active_minutes_mask(full_idx, venue_type)
    active_idx = full_idx[active.values]
    observed_idx = pd.Index(idx)

    missing_active = active_idx.difference(observed_idx)
    observed_inactive = observed_idx.difference(active_idx)
    gap_breaks = 0
    longest_gap = 0
    if len(missing_active):
        mdiff = missing_active.to_series().diff().dt.total_seconds().div(60).fillna(1)
        gap_breaks = int((mdiff != 1).sum())
        seg = (mdiff != 1).cumsum()
        longest_gap = int(missing_active.to_series().groupby(seg).size().max())

    return pd.DataFrame(
        {
            "expected_active_bars": [int(len(active_idx))],
            "observed_bars": [int(len(observed_idx))],
            "missing_active_bars": [int(len(missing_active))],
            "observed_outside_active_bars": [int(len(observed_inactive))],
            "gap_segments": [gap_breaks],
            "longest_missing_active_gap": [longest_gap],
        }
    )
    idx = pd.DatetimeIndex(df["ts_utc"]) 
    active = active_minutes_mask(pd.date_range(idx.min(), idx.max(), freq="min", tz="UTC"), venue_type)
    expected_idx = active.index[active.values]
    observed = pd.Index(idx)
    missing = expected_idx.difference(observed)
    return pd.DataFrame({
        "expected_active_bars": [len(expected_idx)],
        "observed_bars": [len(observed)],
        "missing_active_bars": [len(missing)],
        "gap_segments": [int((missing.to_series().diff().dt.total_seconds().fillna(60) != 60).sum()) if len(missing) else 0],
    })


def stale_quote_flags(df: pd.DataFrame, close_run: int = 10, zero_range_run: int = 10) -> pd.DataFrame:
    out = df.copy()
    same_close = out["close"].eq(out["close"].shift(1))
    zrange = out["high"].eq(out["low"])

    g1 = (same_close != same_close.shift(1)).cumsum()
    g2 = (zrange != zrange.shift(1)).cumsum()
    out["stale_close_flag"] = same_close & (same_close.groupby(g1).transform("size") >= close_run)
    out["stale_zero_range_flag"] = zrange & (zrange.groupby(g2).transform("size") >= zero_range_run)
    return out
