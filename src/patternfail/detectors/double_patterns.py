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
        if mid < 0 or mid >= len(bars):
            continue
        atr_v = bars.iloc[mid]["atr"]
        atr = float(atr_v) if pd.notna(atr_v) else 0.0
        depth = (min(x1, x3) - x2) if top else (x2 - max(x1, x3))
        if depth < min_depth_atr * atr:
            continue
        confirm = None
        p3 = int(s["pivot_index"].iloc[2])
        for j in range(p3 + 1, len(bars)):
            c_v = bars.iloc[j]["close"]
            a_v = bars.iloc[j]["atr"]
            c = float(c_v)
            a = float(a_v) if pd.notna(a_v) else 0.0
            if (top and c < x2 - beta_atr * a) or ((not top) and c > x2 + beta_atr * a):
                confirm = j
                break
        if confirm is None:
            continue
        pivots_used = [
            {
                "label": f"P{k + 1}",
                "pivot_type": s["pivot_type"].iloc[k],
                "pivot_index": int(s["pivot_index"].iloc[k]),
                "ts_utc": str(s["ts_utc"].iloc[k]),
                "pivot_price": float(s["pivot_price"].iloc[k]),
            }
            for k in range(3)
        ]
        out.append({
            "pattern_id": str(uuid.uuid4()), "asset": bars["asset"].iloc[0], "venue_type": bars["venue_type"].iloc[0], "timeframe": bars["timeframe"].iloc[0],
            "pattern_type": "DT" if top else "DB", "direction": "SHORT" if top else "LONG",
            "t_start_utc": s["ts_utc"].iloc[0], "t_end_utc": s["ts_utc"].iloc[2], "t_confirm_utc": bars.iloc[confirm]["ts_utc"],
            "score": float(sim),
            "geometry_params": {
                "similarity": float(sim),
                "depth": float(depth),
                "pivots_used": pivots_used,
                "candidate_window_bounds": {"start_idx": int(s["pivot_index"].iloc[0]), "end_idx": int(s["pivot_index"].iloc[2])},
                "fitted_lines": {
                    "neckline": {
                        "kind": "horizontal",
                        "value": float(x2),
                        "coordinate_system": "raw_price",
                    }
                },
                "score_components": {
                    "peak_similarity": float(sim),
                    "depth_atr_scaled": float(depth / max(atr, 1e-12)),
                    "final_score": float(sim),
                },
                "confirmation_reason": "neckline_breach_atr_filtered",
                "detection_status": "CONFIRMED",
                "detector_variant": "pivot_double_pattern",
            },
            "detector_family": "geometric", "detector_name": "double_patterns", "context_labels": {}, "nested_in_pattern_id": None, "outcome_labels": None,
        })
    return pd.DataFrame(out)
