from __future__ import annotations

import uuid

import numpy as np
import pandas as pd

from .schema import Direction, PatternInstance, PatternType, VenueType


def detect_channels(df: pd.DataFrame, sax_df: pd.DataFrame, timeframe: str, window: int = 80, w1: float = 0.7, w2: float = 0.3, threshold: float = 0.75) -> list[PatternInstance]:
    out = []
    lprice = np.log(df["close"].to_numpy())
    for row in sax_df.itertuples(index=False):
        end_idx = int(row.end_idx)
        start_idx = end_idx - window + 1
        if start_idx < 0:
            continue
        y = lprice[start_idx : end_idx + 1]
        t = np.arange(len(y))
        slope, intercept = np.polyfit(t, y, 1)
        resid = y - (slope * t + intercept)
        sigma = float(np.std(resid))
        width = float(np.quantile(y, 0.95) - np.quantile(y, 0.05))
        score = float(w1 * sigma / max(width, 1e-12) + w2 * row.smoothness)
        if score > threshold:
            continue
        out.append(PatternInstance(
            pattern_id=str(uuid.uuid4()),
            asset=df["asset"].iloc[0],
            venue_type=VenueType(df["venue_type"].iloc[0]),
            timeframe=timeframe,
            pattern_type=PatternType.CHANNEL,
            t_start=df.loc[start_idx, "ts_utc"],
            t_end=df.loc[end_idx, "ts_utc"],
            t_confirm=df.loc[end_idx, "ts_utc"],
            direction=Direction.LONG if slope >= 0 else Direction.SHORT,
            geometry_params={"slope": float(slope), "intercept": float(intercept), "sigma": sigma, "width": width},
            score=score,
            score_components={"regression_sigma_over_width": sigma / max(width, 1e-12), "symbolic_smoothness": float(row.smoothness)},
        ))
    return out
