from __future__ import annotations

import pandas as pd


def add_true_range_and_atr(df: pd.DataFrame, period: int = 14) -> pd.DataFrame:
    out = df.copy()
    prev = out["close"].shift(1)
    out["true_range"] = pd.concat([
        out["high"] - out["low"],
        (out["high"] - prev).abs(),
        (out["low"] - prev).abs(),
    ], axis=1).max(axis=1)
    out["atr"] = out["true_range"].rolling(period, min_periods=period).mean()
    return out
