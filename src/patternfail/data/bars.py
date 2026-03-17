from __future__ import annotations

import pandas as pd

from .market_hours import active_minutes_mask


def aggregate_bars(df_1m: pd.DataFrame, timeframe: str, venue_type: str) -> pd.DataFrame:
    d = df_1m.copy().set_index("ts_utc").sort_index()
    mask = active_minutes_mask(d.index, venue_type)
    d = d.loc[mask.values]

    out = d.resample(timeframe, label="left", closed="left").agg(
        open=("open", "first"),
        high=("high", "max"),
        low=("low", "min"),
        close=("close", "last"),
        volume=("volume", "sum"),
        n_underlying=("open", "size"),
    ).dropna().reset_index()
    tf_mins = {"3min": 3, "5min": 5, "15min": 15, "1h": 60, "4h": 240, "1d": 1440, "1w": 10080}[timeframe]
    out["completeness_flag"] = out["n_underlying"] >= 0.95 * tf_mins
    out["gap_flag"] = ~out["completeness_flag"]
    out["timeframe"] = timeframe
    out["asset"] = df_1m["asset"].iloc[0]
    out["venue_type"] = venue_type
    return out
