from __future__ import annotations

import pandas as pd


def extract_pivots(df: pd.DataFrame, atr_lambda: float = 2.0, min_sep: int = 3) -> pd.DataFrame:
    x = df.reset_index(drop=True)
    pivots = []
    direction = None
    ext_i = 0
    ext_p = float(x.loc[0, "close"])
    last_pivot_i = -10_000

    for i in range(1, len(x)):
        p = float(x.loc[i, "close"])
        atr = float(x.loc[i, "atr"]) if pd.notna(x.loc[i, "atr"]) else 0.0
        d = atr_lambda * atr
        if direction is None:
            direction = "up" if p >= ext_p else "down"
            ext_i, ext_p = i, p
            continue

        if direction == "up":
            if p >= ext_p:
                ext_i, ext_p = i, p
            elif ext_p - p >= d and ext_i - last_pivot_i >= min_sep and d > 0:
                prev = float(x.loc[last_pivot_i, "close"]) if last_pivot_i >= 0 else ext_p
                pivots.append({
                    "asset": x.loc[ext_i, "asset"],
                    "timeframe": x.loc[ext_i, "timeframe"],
                    "pivot_index": ext_i,
                    "ts_utc": x.loc[ext_i, "ts_utc"],
                    "pivot_type": "HIGH",
                    "pivot_price": ext_p,
                    "local_atr": atr,
                    "swing_magnitude": abs(ext_p - prev),
                })
                last_pivot_i = ext_i
                direction = "down"
                ext_i, ext_p = i, p
        else:
            if p <= ext_p:
                ext_i, ext_p = i, p
            elif p - ext_p >= d and ext_i - last_pivot_i >= min_sep and d > 0:
                prev = float(x.loc[last_pivot_i, "close"]) if last_pivot_i >= 0 else ext_p
                pivots.append({
                    "asset": x.loc[ext_i, "asset"],
                    "timeframe": x.loc[ext_i, "timeframe"],
                    "pivot_index": ext_i,
                    "ts_utc": x.loc[ext_i, "ts_utc"],
                    "pivot_type": "LOW",
                    "pivot_price": ext_p,
                    "local_atr": atr,
                    "swing_magnitude": abs(ext_p - prev),
                })
                last_pivot_i = ext_i
                direction = "up"
                ext_i, ext_p = i, p

    return pd.DataFrame(pivots)
