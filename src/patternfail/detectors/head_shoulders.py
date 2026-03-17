from __future__ import annotations

import uuid

import numpy as np
import pandas as pd


def _inst(row0, pattern_type, direction, t_start, t_end, t_confirm, score, geom, name):
    return {
        "pattern_id": str(uuid.uuid4()),
        "asset": row0["asset"],
        "venue_type": row0["venue_type"],
        "timeframe": row0["timeframe"],
        "pattern_type": pattern_type,
        "direction": direction,
        "t_start_utc": t_start,
        "t_end_utc": t_end,
        "t_confirm_utc": t_confirm,
        "score": score,
        "geometry_params": geom,
        "detector_family": "geometric",
        "detector_name": name,
        "context_labels": {},
        "nested_in_pattern_id": None,
        "outcome_labels": None,
    }


def detect_hs_lo(bars: pd.DataFrame, pivots: pd.DataFrame, shoulder_tol: float, trough_tol: float, beta_atr: float) -> pd.DataFrame:
    if pivots.empty:
        return pd.DataFrame()
    out = []
    p = pivots.reset_index(drop=True)
    for i in range(len(p) - 4):
        s = p.iloc[i : i + 5]
        kinds = tuple(1 if t == "HIGH" else -1 for t in s["pivot_type"])
        if kinds not in [(1, -1, 1, -1, 1), (-1, 1, -1, 1, -1)]:
            continue
        x = s["pivot_price"].to_numpy()
        top = kinds[0] == 1
        if top and not (x[2] > x[0] and x[2] > x[4]):
            continue
        if (not top) and not (x[2] < x[0] and x[2] < x[4]):
            continue
        shoulder = abs(x[0] - x[4]) / max((abs(x[0]) + abs(x[4])) / 2, 1e-12)
        trough = abs(x[1] - x[3]) / max((abs(x[1]) + abs(x[3])) / 2, 1e-12)
        if shoulder > shoulder_tol or trough > trough_tol:
            continue

        t2, t4 = int(s["pivot_index"].iloc[1]), int(s["pivot_index"].iloc[3])
        if t4 == t2:
            continue
        m = (x[3] - x[1]) / (t4 - t2)
        p5 = int(s["pivot_index"].iloc[4])
        confirm = None
        for j in range(p5 + 1, len(bars)):
            neckline = x[1] + m * (j - t2)
            atr = float(bars.loc[j, "atr"]) if pd.notna(bars.loc[j, "atr"]) else 0.0
            c = float(bars.loc[j, "close"])
            if (top and c < neckline - beta_atr * atr) or ((not top) and c > neckline + beta_atr * atr):
                confirm = j
                break
        if confirm is None:
            continue

        vec = (x - x[1]) / (abs(x[2] - x[1]) + 1e-12)
        tpl = np.array([0.7, 0, 1.0, 0, 0.7]) if top else np.array([-0.7, 0, -1.0, 0, -0.7])
        score = float(np.sqrt(np.mean((vec - tpl) ** 2)))
        mm = abs(x[2] - (x[1] + m * (int(s["pivot_index"].iloc[2]) - t2)))
        out.append(_inst(
            bars.iloc[0],
            "HS_TOP" if top else "HS_INV",
            "SHORT" if top else "LONG",
            s["ts_utc"].iloc[0], s["ts_utc"].iloc[4], bars.loc[confirm, "ts_utc"], score,
            {
                "left_shoulder": float(x[0]), "head": float(x[2]), "right_shoulder": float(x[4]),
                "neckline_slope": float(m), "neckline_intercept": float(x[1] - m * t2),
                "shoulder_similarity": float(shoulder), "trough_similarity": float(trough),
                "measured_move_height": float(mm),
            },
            "hs_lo",
        ))
    return pd.DataFrame(out)


def detect_hs_ieee_comparator(bars: pd.DataFrame, keypoint_window: int = 180, prominence_atr: float = 0.7) -> pd.DataFrame:
    # Lightweight key-point sliding-window comparator; same output schema.
    out = []
    close = bars["close"].to_numpy()
    atr = bars["atr"].fillna(0).to_numpy()
    for end in range(keypoint_window, len(bars), max(keypoint_window // 4, 1)):
        start = end - keypoint_window
        w = close[start:end]
        idx = np.arange(len(w))
        k = np.argpartition(w, -3)[-3:]
        k = np.sort(k)
        if len(k) < 3:
            continue
        l, h, r = k[0], k[1], k[2]
        if not (w[h] > w[l] and w[h] > w[r]):
            continue
        shoulder_sim = abs(w[l] - w[r]) / max((w[l] + w[r]) / 2, 1e-12)
        if shoulder_sim > 0.15:
            continue
        if (w[h] - max(w[l], w[r])) < prominence_atr * max(float(np.nanmean(atr[start:end])), 1e-12):
            continue
        confirm_i = end
        if confirm_i >= len(bars):
            continue
        out.append(_inst(
            bars.iloc[0], "HS_TOP_IEEE", "SHORT", bars.loc[start + l, "ts_utc"], bars.loc[start + r, "ts_utc"], bars.loc[confirm_i, "ts_utc"],
            float(shoulder_sim), {"window": keypoint_window, "left": float(w[l]), "head": float(w[h]), "right": float(w[r])}, "hs_ieee"
        ))
    return pd.DataFrame(out)
