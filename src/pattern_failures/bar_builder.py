from __future__ import annotations

import pandas as pd

from .market_calendars import trading_mask
from .schema import VenueType


AGG = {"open": "first", "high": "max", "low": "min", "close": "last", "volume": "sum"}


def build_timeframe(df: pd.DataFrame, timeframe: str, venue_type: VenueType) -> pd.DataFrame:
    dfx = df.copy().set_index("ts_utc").sort_index()
    mask = trading_mask(dfx.index, venue_type)
    dfx = dfx.loc[mask.values]
    out = dfx.resample(timeframe, label="left", closed="left").agg(AGG).dropna()
    out = out.reset_index()
    out["asset"] = df["asset"].iloc[0]
    out["venue_type"] = venue_type.value
    return out
