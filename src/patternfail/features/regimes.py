from __future__ import annotations

import numpy as np
import pandas as pd


def fit_regime_quantiles(train_df: pd.DataFrame, q=(0.33, 0.66)) -> tuple[float, float]:
    lo, hi = train_df["rv"].quantile([q[0], q[1]]).tolist()
    return float(lo), float(hi)


def apply_regimes(df: pd.DataFrame, q_lo: float, q_hi: float) -> pd.DataFrame:
    out = df.copy()
    out["vol_regime"] = np.select([out["rv"] <= q_lo, out["rv"] >= q_hi], ["LOW", "HIGH"], default="MID")
    return out
