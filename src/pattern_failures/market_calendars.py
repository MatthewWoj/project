from __future__ import annotations

import pandas as pd

from .schema import VenueType


def trading_mask(index: pd.DatetimeIndex, venue_type: VenueType) -> pd.Series:
    if venue_type == VenueType.CRYPTO:
        return pd.Series(True, index=index)
    if venue_type in (VenueType.FX, VenueType.METAL):
        return pd.Series(index.dayofweek < 5, index=index)
    et = index.tz_convert("America/New_York")
    in_weekday = et.dayofweek < 5
    open_time = (et.hour > 9) | ((et.hour == 9) & (et.minute >= 30))
    close_time = et.hour < 16
    return pd.Series(in_weekday & open_time & close_time, index=index)


def classify_gap(start: pd.Timestamp, end: pd.Timestamp, venue_type: VenueType) -> str:
    if venue_type == VenueType.CRYPTO:
        return "suspicious"
    if venue_type in (VenueType.FX, VenueType.METAL):
        if start.dayofweek >= 4 and end.dayofweek <= 0:
            return "expected_weekend"
    if venue_type == VenueType.EQUITY_US:
        return "expected_non_session"
    return "unexpected"
