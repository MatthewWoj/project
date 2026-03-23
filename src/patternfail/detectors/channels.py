from __future__ import annotations

import uuid

import numpy as np
import pandas as pd

from .symbolic_sax import ascii_differences, sax_word, smoothness


def detect_symbolic_channels(
    bars: pd.DataFrame,
    sax_window: int,
    paa_segments: int,
    alphabet_size: int,
    residual_weight: float,
    smoothness_weight: float,
    threshold: float,
    max_residual_ratio: float = 0.45,
    min_r2: float = 0.6,
) -> pd.DataFrame:
    if len(bars) < sax_window:
        return pd.DataFrame()
    out = []
    lprice = np.log(bars["close"].to_numpy())
    for end in range(sax_window - 1, len(bars)):
        start = end - sax_window + 1
        y = lprice[start : end + 1]
        t = np.arange(len(y))
        slope, intercept = np.polyfit(t, y, 1)
        resid = y - (slope * t + intercept)
        sigma = float(np.std(resid))
        width = float(np.quantile(y, 0.95) - np.quantile(y, 0.05))
        ss_tot = float(np.sum((y - y.mean()) ** 2))
        ss_res = float(np.sum(resid ** 2))
        r2 = 1.0 - ss_res / max(ss_tot, 1e-12)
        word = sax_word(y, paa_segments, alphabet_size)
        ascii_diff = ascii_differences(word)
        sm = smoothness(word)
        symbolic_variability = float(np.std(ascii_diff)) if ascii_diff else 0.0
        residual_ratio = sigma / max(width, 1e-12)
        score = residual_weight * residual_ratio + smoothness_weight * sm + 0.15 * symbolic_variability + 0.1 * max(0.0, 1.0 - r2)
        if residual_ratio > max_residual_ratio or r2 < min_r2:
            continue
        if score > threshold:
            continue
        half_width = max(1.5 * sigma, 1e-8)
        out.append({
            "pattern_id": str(uuid.uuid4()), "asset": bars["asset"].iloc[0], "venue_type": bars["venue_type"].iloc[0], "timeframe": bars["timeframe"].iloc[0],
            "pattern_type": "CHANNEL", "direction": "LONG" if slope >= 0 else "SHORT",
            "t_start_utc": bars.loc[start, "ts_utc"], "t_end_utc": bars.loc[end, "ts_utc"], "t_confirm_utc": bars.loc[end, "ts_utc"],
            "score": float(score),
            "geometry_params": {
                "slope": float(slope),
                "intercept": float(intercept),
                "channel_half_width": float(half_width),
                "sigma": sigma,
                "width": width,
                "sax_word": word,
                "ascii_diff": ascii_diff,
                "smoothness": sm,
                "symbolic_variability": symbolic_variability,
                "r2": float(r2),
                "residual_ratio": float(residual_ratio),
                "candidate_window_bounds": {
                    "start_idx": int(start),
                    "end_idx": int(end),
                    "start_ts": str(bars.loc[start, "ts_utc"]),
                    "end_ts": str(bars.loc[end, "ts_utc"]),
                },
                "fitted_lines": {
                    "center": {
                        "kind": "affine",
                        "slope": float(slope),
                        "intercept": float(intercept),
                        "coordinate_system": "log_price",
                        "index_mode": "bars_since_start",
                        "start_ts": str(bars.loc[start, "ts_utc"]),
                        "end_ts": str(bars.loc[end, "ts_utc"]),
                    },
                    "upper": {
                        "kind": "affine",
                        "slope": float(slope),
                        "intercept": float(intercept + half_width),
                        "coordinate_system": "log_price",
                        "index_mode": "bars_since_start",
                        "start_ts": str(bars.loc[start, "ts_utc"]),
                        "end_ts": str(bars.loc[end, "ts_utc"]),
                    },
                    "lower": {
                        "kind": "affine",
                        "slope": float(slope),
                        "intercept": float(intercept - half_width),
                        "coordinate_system": "log_price",
                        "index_mode": "bars_since_start",
                        "start_ts": str(bars.loc[start, "ts_utc"]),
                        "end_ts": str(bars.loc[end, "ts_utc"]),
                    },
                    "center": {"slope": float(slope), "intercept": float(intercept)},
                    "upper": {"slope": float(slope), "intercept": float(intercept + half_width)},
                    "lower": {"slope": float(slope), "intercept": float(intercept - half_width)},
                },
                "score_components": {
                    "residual_ratio": float(residual_ratio),
                    "smoothness": float(sm),
                    "symbolic_variability": float(symbolic_variability),
                    "r2_penalty": float(max(0.0, 1.0 - r2)),
                },
                "confirmation_reason": "structural_window_end",
                "detection_status": "STRUCTURAL_ONLY",
                "detector_variant": "cpc_sax_inspired_channel",
            },
            "detector_family": "symbolic", "detector_name": "sax_channel", "context_labels": {}, "nested_in_pattern_id": None, "outcome_labels": None,
        })
    return pd.DataFrame(out)
