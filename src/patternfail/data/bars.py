from __future__ import annotations

import pandas as pd

from .market_hours import active_minutes_mask


_TIMEFRAME_ALIASES = {
    "1m": ("1min", 1),
    "1min": ("1min", 1),
    "3min": ("3min", 3),
    "5min": ("5min", 5),
    "15min": ("15min", 15),
    "1h": ("1h", 60),
    "4h": ("4h", 240),
    "1d": ("1d", 1440),
    "1w": ("1w", 10080),
}


def _normalize_timeframe(timeframe: str) -> tuple[str, int]:
    try:
        return _TIMEFRAME_ALIASES[timeframe]
    except KeyError as exc:
        supported = ", ".join(_TIMEFRAME_ALIASES)
        raise ValueError(f"Unsupported timeframe '{timeframe}'. Supported values: {supported}") from exc


def aggregate_bars(df_1m: pd.DataFrame, timeframe: str, venue_type: str) -> pd.DataFrame:
    d = df_1m.copy().set_index("ts_utc").sort_index()
    mask = active_minutes_mask(d.index, venue_type)
    d = d.loc[mask.values]
    resample_freq, tf_mins = _normalize_timeframe(timeframe)

    out = d.resample(resample_freq, label="left", closed="left").agg(
        open=("open", "first"),
        high=("high", "max"),
        low=("low", "min"),
        close=("close", "last"),
        volume=("volume", "sum"),
        n_underlying=("open", "size"),
    ).dropna().reset_index()
    out["completeness_flag"] = out["n_underlying"] >= 0.95 * tf_mins
    out["gap_flag"] = ~out["completeness_flag"]
    out["timeframe"] = timeframe
    out["asset"] = df_1m["asset"].iloc[0]
    out["venue_type"] = venue_type
    return out
