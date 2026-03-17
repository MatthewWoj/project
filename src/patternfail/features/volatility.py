from __future__ import annotations

import pandas as pd


def add_realized_vol(df: pd.DataFrame, window: int = 60) -> pd.DataFrame:
    out = df.copy()
    out["rv"] = out["log_ret"].rolling(window).std()
    return out
