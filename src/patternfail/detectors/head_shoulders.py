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
        pivots_used = [
            {
                "label": f"P{k + 1}",
                "pivot_type": s["pivot_type"].iloc[k],
                "pivot_index": int(s["pivot_index"].iloc[k]),
                "ts_utc": str(s["ts_utc"].iloc[k]),
                "pivot_price": float(x[k]),
            }
            for k in range(5)
        ]
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
                "pivots_used": pivots_used,
                "candidate_window_bounds": {"start_idx": int(s["pivot_index"].iloc[0]), "end_idx": int(s["pivot_index"].iloc[4])},
                "fitted_lines": {
                    "neckline": {
                        "kind": "affine",
                        "slope": float(m),
                        "intercept": float(x[1] - m * t2),
                        "coordinate_system": "raw_price",
                        "index_mode": "global",
                    }
                },
                "score_components": {"template_rmse": float(score), "shoulder_similarity": float(shoulder), "trough_similarity": float(trough)},
                "confirmation_reason": "neckline_breach_atr_filtered",
                "detection_status": "CONFIRMED",
                "detector_variant": "lo_pivot_template",
            },
            "hs_lo",
        ))
    return pd.DataFrame(out)


def detect_hs_ieee_comparator(bars: pd.DataFrame, keypoint_window: int = 180, prominence_atr: float = 0.7) -> pd.DataFrame:
    # Research-prototype comparator inspired by key-point/sliding-window IEEE detector.
    out = []
    close = bars["close"].to_numpy(dtype=float)
    atr = bars["atr"].fillna(0).to_numpy(dtype=float)
    keypoints = []
    for i in range(1, len(bars) - 1):
        prev_p, cur_p, next_p = close[i - 1], close[i], close[i + 1]
        if (cur_p >= prev_p and cur_p > next_p) or (cur_p <= prev_p and cur_p < next_p):
            pivot_type = "HIGH" if cur_p >= prev_p and cur_p > next_p else "LOW"
            neighborhood = close[max(0, i - 3) : min(len(close), i + 4)]
            ref = neighborhood.min() if pivot_type == "HIGH" else neighborhood.max()
            prominence = abs(cur_p - ref)
            if prominence >= prominence_atr * max(atr[i], 1e-12):
                keypoints.append({"idx": i, "price": cur_p, "type": pivot_type, "ts_utc": bars.loc[i, "ts_utc"]})

    if len(keypoints) < 5:
        return pd.DataFrame()

    step = max(keypoint_window // 4, 1)
    for end in range(keypoint_window, len(bars) + 1, step):
        start = max(0, end - keypoint_window)
        window_points = [kp for kp in keypoints if start <= kp["idx"] < end]
        best = None
        for i in range(len(window_points) - 4):
            seq = window_points[i : i + 5]
            types = tuple(pt["type"] for pt in seq)
            if types not in [("HIGH", "LOW", "HIGH", "LOW", "HIGH"), ("LOW", "HIGH", "LOW", "HIGH", "LOW")]:
                continue
            top = types[0] == "HIGH"
            prices = np.array([pt["price"] for pt in seq], dtype=float)
            if top and not (prices[2] > prices[0] and prices[2] > prices[4]):
                continue
            if (not top) and not (prices[2] < prices[0] and prices[2] < prices[4]):
                continue
            shoulder_sim = abs(prices[0] - prices[4]) / max((abs(prices[0]) + abs(prices[4])) / 2, 1e-12)
            trough_sim = abs(prices[1] - prices[3]) / max((abs(prices[1]) + abs(prices[3])) / 2, 1e-12)
            head_prom = abs(prices[2] - np.mean([prices[0], prices[4]])) / max(atr[seq[2]["idx"]], 1e-12)
            if shoulder_sim > 0.15 or trough_sim > 0.2 or head_prom < prominence_atr:
                continue

            i2, i4 = seq[1]["idx"], seq[3]["idx"]
            slope = (prices[3] - prices[1]) / max(i4 - i2, 1)
            intercept = prices[1] - slope * i2
            confirm = None
            for j in range(seq[4]["idx"] + 1, min(end + step, len(bars))):
                neckline = slope * j + intercept
                c = close[j]
                a = atr[j]
                if (top and c < neckline - 0.1 * a) or ((not top) and c > neckline + 0.1 * a):
                    confirm = j
                    break
            if confirm is None:
                continue

            fit = float(0.5 * shoulder_sim + 0.3 * trough_sim + 0.2 / max(head_prom, 1e-12))
            candidate = {
                "score": fit,
                "pattern_type": "HS_TOP_IEEE" if top else "HS_INV_IEEE",
                "direction": "SHORT" if top else "LONG",
                "t_start_utc": seq[0]["ts_utc"],
                "t_end_utc": seq[4]["ts_utc"],
                "t_confirm_utc": bars.loc[confirm, "ts_utc"],
                "geometry_params": {
                    "window": keypoint_window,
                    "pivots_used": [
                        {
                            "label": f"P{k + 1}",
                            "pivot_type": seq[k]["type"],
                            "pivot_index": int(seq[k]["idx"]),
                            "ts_utc": str(seq[k]["ts_utc"]),
                            "pivot_price": float(seq[k]["price"]),
                        }
                        for k in range(5)
                    ],
                    "candidate_window_bounds": {"start_idx": int(start), "end_idx": int(end - 1)},
                    "fitted_lines": {
                        "neckline": {
                            "kind": "affine",
                            "slope": float(slope),
                            "intercept": float(intercept),
                            "coordinate_system": "raw_price",
                            "index_mode": "global",
                        }
                    },
                    "score_components": {
                        "shoulder_similarity": float(shoulder_sim),
                        "trough_similarity": float(trough_sim),
                        "head_prominence_atr": float(head_prom),
                        "fit_score": float(fit),
                    },
                    "confirmation_reason": "keypoint_window_neckline_breach",
                    "detection_status": "CONFIRMED",
                    "detector_variant": "ieee_keypoint_window",
                },
            }
            if best is None or candidate["score"] < best["score"]:
                best = candidate
        if best is None:
            continue
        out.append(_inst(
            bars.iloc[0],
            best["pattern_type"],
            best["direction"],
            best["t_start_utc"],
            best["t_end_utc"],
            best["t_confirm_utc"],
            best["score"],
            best["geometry_params"],
            "hs_ieee",
        ))
    return pd.DataFrame(out)
