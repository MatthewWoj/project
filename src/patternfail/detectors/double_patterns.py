from __future__ import annotations

import uuid

import pandas as pd


def detect_double_patterns(bars: pd.DataFrame, pivots: pd.DataFrame, peak_tol: float, min_depth_atr: float, beta_atr: float) -> pd.DataFrame:
    if pivots.empty:
        return pd.DataFrame()
    out = []
    p = pivots.reset_index(drop=True)
    for i in range(len(p) - 2):
        s = p.iloc[i : i + 3]
        t = tuple(s["pivot_type"])
        if t not in [("HIGH", "LOW", "HIGH"), ("LOW", "HIGH", "LOW")]:
            continue
        top = t[0] == "HIGH"
        x1, x2, x3 = s["pivot_price"].to_list()
        sim = abs(x1 - x3) / max((abs(x1) + abs(x3)) / 2, 1e-12)
        if sim > peak_tol:
            continue
        mid = int(s["pivot_index"].iloc[1])
        atr = float(bars.loc[mid, "atr"]) if pd.notna(bars.loc[mid, "atr"]) else 0.0
        depth = (min(x1, x3) - x2) if top else (x2 - max(x1, x3))
        if depth < min_depth_atr * atr:
            continue
        confirm = None
        p3 = int(s["pivot_index"].iloc[2])
        for j in range(p3 + 1, len(bars)):
            c = float(bars.loc[j, "close"])
            a = float(bars.loc[j, "atr"]) if pd.notna(bars.loc[j, "atr"]) else 0.0
            if (top and c < x2 - beta_atr * a) or ((not top) and c > x2 + beta_atr * a):
                confirm = j
                break
        if confirm is None:
            continue
        out.append({
            "pattern_id": str(uuid.uuid4()), "asset": bars["asset"].iloc[0], "venue_type": bars["venue_type"].iloc[0], "timeframe": bars["timeframe"].iloc[0],
            "pattern_type": "DT" if top else "DB", "direction": "SHORT" if top else "LONG",
            "t_start_utc": s["ts_utc"].iloc[0], "t_end_utc": s["ts_utc"].iloc[2], "t_confirm_utc": bars.loc[confirm, "ts_utc"],
            "score": float(sim), "geometry_params": {"similarity": float(sim), "depth": float(depth)},
            "detector_family": "geometric", "detector_name": "double_patterns", "context_labels": {}, "nested_in_pattern_id": None, "outcome_labels": None,
        })
    return pd.DataFrame(out)
