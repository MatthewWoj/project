from __future__ import annotations

import uuid

import numpy as np
import pandas as pd

from .symbolic_sax import sax_word, smoothness


def detect_symbolic_channels(bars: pd.DataFrame, sax_window: int, paa_segments: int, alphabet_size: int, residual_weight: float, smoothness_weight: float, threshold: float) -> pd.DataFrame:
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
        word = sax_word(np.diff(y, prepend=y[0]), paa_segments, alphabet_size)
        sm = smoothness(word)
        score = residual_weight * (sigma / max(width, 1e-12)) + smoothness_weight * sm
        if score > threshold:
            continue
        out.append({
            "pattern_id": str(uuid.uuid4()), "asset": bars["asset"].iloc[0], "venue_type": bars["venue_type"].iloc[0], "timeframe": bars["timeframe"].iloc[0],
            "pattern_type": "CHANNEL", "direction": "LONG" if slope >= 0 else "SHORT",
            "t_start_utc": bars.loc[start, "ts_utc"], "t_end_utc": bars.loc[end, "ts_utc"], "t_confirm_utc": bars.loc[end, "ts_utc"],
            "score": float(score),
            "geometry_params": {"slope": float(slope), "intercept": float(intercept), "sigma": sigma, "width": width, "sax_word": word, "smoothness": sm},
            "detector_family": "symbolic", "detector_name": "sax_channel", "context_labels": {}, "nested_in_pattern_id": None, "outcome_labels": None,
        })
    return pd.DataFrame(out)
