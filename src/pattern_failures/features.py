from __future__ import annotations

import numpy as np
import pandas as pd


def add_atr(df: pd.DataFrame, n: int = 14) -> pd.DataFrame:
    x = df.copy()
    prev_close = x["close"].shift(1)
    tr = pd.concat([
        x["high"] - x["low"],
        (x["high"] - prev_close).abs(),
        (x["low"] - prev_close).abs(),
    ], axis=1).max(axis=1)
    x["atr"] = tr.rolling(n, min_periods=n).mean()
    return x


def add_returns_and_regimes(df: pd.DataFrame, rv_window: int = 60) -> pd.DataFrame:
    x = df.copy()
    x["log_ret"] = np.log(x["close"]).diff()
    x["rv"] = x["log_ret"].rolling(rv_window).std()
    q1, q2 = x["rv"].quantile([0.33, 0.66])
    x["vol_regime"] = np.select([x["rv"] <= q1, x["rv"] >= q2], ["LOW", "HIGH"], default="MID")
    return x
