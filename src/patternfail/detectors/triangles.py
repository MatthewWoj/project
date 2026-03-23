from __future__ import annotations

import uuid

import numpy as np
import pandas as pd


def detect_triangles(bars: pd.DataFrame, pivots: pd.DataFrame, window_pivots: int, convergence_ratio: float, residual_eta_atr: float, beta_atr: float) -> pd.DataFrame:
    if pivots.empty or len(pivots) < window_pivots:
        return pd.DataFrame()
    out = []
    p = pivots.reset_index(drop=True)
    for i in range(len(p) - window_pivots + 1):
        s = p.iloc[i : i + window_pivots]
        hi = s[s["pivot_type"] == "HIGH"]
        lo = s[s["pivot_type"] == "LOW"]
        if len(hi) < 2 or len(lo) < 2:
            continue
        hu = hi["pivot_index"].to_numpy(); hp = hi["pivot_price"].to_numpy()
        lu = lo["pivot_index"].to_numpy(); lp = lo["pivot_price"].to_numpy()
        au, bu = np.polyfit(hu, hp, 1)
        al, bl = np.polyfit(lu, lp, 1)
        ts = int(s["pivot_index"].iloc[0]); te = int(s["pivot_index"].iloc[-1])
        if ts < 0 or te < 0 or ts >= len(bars) or te >= len(bars):
            continue
        ws = (au * ts + bu) - (al * ts + bl)
        we = (au * te + bu) - (al * te + bl)
        if ws <= 0 or we > convergence_ratio * ws:
            continue
        ru = float(np.max(np.abs(hp - (au * hu + bu))))
        rl = float(np.max(np.abs(lp - (al * lu + bl))))
        atr_v = bars.iloc[te]["atr"]
        atr = float(atr_v) if pd.notna(atr_v) else 1.0
        if max(ru, rl) > residual_eta_atr * atr:
            continue
        confirm = None; direction = None
        for j in range(te + 1, len(bars)):
            c_v = bars.iloc[j]["close"]
            a_v = bars.iloc[j]["atr"]
            c = float(c_v)
            a = float(a_v) if pd.notna(a_v) else 0.0
            u = au * j + bu; l = al * j + bl
            if c > u + beta_atr * a:
                confirm = j; direction = "LONG"; break
            if c < l - beta_atr * a:
                confirm = j; direction = "SHORT"; break
        if confirm is None:
            continue
        ptype = "TRIANGLE_GENERIC"
        if abs(au) < 1e-3 and al > 0: ptype = "TRIANGLE_ASC"
        elif abs(al) < 1e-3 and au < 0: ptype = "TRIANGLE_DESC"
        elif au < 0 and al > 0: ptype = "TRIANGLE_SYM"
        out.append({
            "pattern_id": str(uuid.uuid4()), "asset": bars["asset"].iloc[0], "venue_type": bars["venue_type"].iloc[0], "timeframe": bars["timeframe"].iloc[0],
            "pattern_type": ptype, "direction": direction,
            "t_start_utc": s["ts_utc"].iloc[0], "t_end_utc": s["ts_utc"].iloc[-1], "t_confirm_utc": bars.iloc[confirm]["ts_utc"],
            "score": float(max(ru, rl) / max(ws, 1e-12) + we / max(ws, 1e-12)),
            "geometry_params": {"a_u": float(au), "b_u": float(bu), "a_l": float(al), "b_l": float(bl), "width_start": float(ws), "width_end": float(we), "residual": float(max(ru, rl))},
            "detector_family": "geometric", "detector_name": "triangles", "context_labels": {}, "nested_in_pattern_id": None, "outcome_labels": None,
        })
    return pd.DataFrame(out)
