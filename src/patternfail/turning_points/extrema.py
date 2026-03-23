from __future__ import annotations

import pandas as pd


def extract_pivots_smoothed_extrema(
    df: pd.DataFrame,
    smoothing_window: int = 5,
    prominence_atr: float = 0.8,
    min_sep: int = 3,
) -> pd.DataFrame:
    x = df.reset_index(drop=True).copy()
    if x.empty:
        return pd.DataFrame()

    smooth = x["close"].rolling(smoothing_window, center=True, min_periods=1).mean()
    pivots = []
    last_idx = -10_000

    for i in range(1, len(x) - 1):
        left, cur, right = float(smooth.iloc[i - 1]), float(smooth.iloc[i]), float(smooth.iloc[i + 1])
        if i - last_idx < min_sep:
            continue

        pivot_type = None
        if cur >= left and cur > right:
            pivot_type = "HIGH"
        elif cur <= left and cur < right:
            pivot_type = "LOW"
        if pivot_type is None:
            continue

        atr_v = x.loc[i, "atr"]
        atr = float(atr_v) if pd.notna(atr_v) else 0.0
        if atr <= 0:
            continue

        neighborhood = smooth.iloc[max(0, i - smoothing_window) : min(len(x), i + smoothing_window + 1)]
        ref = float(neighborhood.min()) if pivot_type == "HIGH" else float(neighborhood.max())
        prominence = abs(cur - ref)
        if prominence < prominence_atr * atr:
            continue

        pivots.append(
            {
                "asset": x.loc[i, "asset"],
                "timeframe": x.loc[i, "timeframe"],
                "pivot_index": i,
                "ts_utc": x.loc[i, "ts_utc"],
                "pivot_type": pivot_type,
                "pivot_price": float(x.loc[i, "close"]),
                "local_atr": atr,
                "swing_magnitude": prominence,
                "pivot_method": "smoothed_extrema",
            }
        )
        last_idx = i

    return pd.DataFrame(pivots)
