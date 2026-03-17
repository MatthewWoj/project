from __future__ import annotations

import pandas as pd


def atr_zigzag_pivots(df: pd.DataFrame, lam: float = 2.0) -> pd.DataFrame:
    if "atr" not in df:
        raise ValueError("ATR column required")
    x = df.reset_index(drop=True)
    pivots = []
    direction = None
    ext_idx = 0
    ext_price = x.loc[0, "close"]

    for i in range(1, len(x)):
        price = x.loc[i, "close"]
        delta = lam * (x.loc[i, "atr"] if pd.notna(x.loc[i, "atr"]) else 0.0)
        if direction is None:
            if price > ext_price:
                direction = "up"
                ext_idx, ext_price = i, price
            elif price < ext_price:
                direction = "down"
                ext_idx, ext_price = i, price
            continue

        if direction == "up":
            if price >= ext_price:
                ext_idx, ext_price = i, price
            elif ext_price - price >= delta > 0:
                pivots.append((ext_idx, x.loc[ext_idx, "ts_utc"], ext_price, 1))
                direction = "down"
                ext_idx, ext_price = i, price
        else:
            if price <= ext_price:
                ext_idx, ext_price = i, price
            elif price - ext_price >= delta > 0:
                pivots.append((ext_idx, x.loc[ext_idx, "ts_utc"], ext_price, -1))
                direction = "up"
                ext_idx, ext_price = i, price

    return pd.DataFrame(pivots, columns=["bar_index", "ts_utc", "price", "kind"])
