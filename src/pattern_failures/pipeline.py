from __future__ import annotations

from .detectors_geometric import detect_double, detect_hs, detect_triangles
from .detectors_symbolic import detect_channels
from .features import add_atr, add_returns_and_regimes
from .symbolic_transform import rolling_sax
from .turning_points import atr_zigzag_pivots


def run_detectors(df, timeframe: str, cfg):
    df = add_atr(df, cfg.thresholds.atr_period)
    df = add_returns_and_regimes(df)
    pivots = atr_zigzag_pivots(df, cfg.thresholds.zigzag_lambda)
    instances = []
    instances.extend(detect_hs(df, pivots, timeframe, cfg.thresholds.shoulder_tolerance, cfg.thresholds.trough_tolerance, cfg.thresholds.neckline_beta))
    instances.extend(detect_double(df, pivots, timeframe, cfg.thresholds.peak_tolerance, cfg.thresholds.neckline_beta, cfg.thresholds.dt_depth_kappa))
    instances.extend(detect_triangles(df, pivots, timeframe, beta=cfg.thresholds.neckline_beta))
    sax_df = rolling_sax(df, window=cfg.sax.window, m=cfg.sax.paa_segments, a=cfg.sax.alphabet_size)
    instances.extend(detect_channels(df, sax_df, timeframe, window=cfg.sax.window, threshold=cfg.thresholds.channel_score_threshold))
    return df, pivots, instances
