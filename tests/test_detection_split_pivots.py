from pathlib import Path

import pandas as pd

from patternfail.common.io import write_table
from patternfail.common.paths import ensure_run_dirs
from patternfail.pipeline import _run_detection_layer


def test_detection_recomputes_pivots_on_test_subset(tmp_path: Path):
    dirs = ensure_run_dirs(str(tmp_path), "run")

    ts = pd.date_range("2025-01-01", periods=300, freq="min", tz="UTC")
    close = pd.Series([100 + ((i % 20) - 10) for i in range(len(ts))], dtype=float)
    bars = pd.DataFrame(
        {
            "ts_utc": ts,
            "open": close.shift(1).fillna(close.iloc[0]),
            "high": close + 1.0,
            "low": close - 1.0,
            "close": close,
            "volume": 1.0,
            "n_underlying": 1,
            "completeness_flag": True,
            "gap_flag": False,
            "timeframe": "15min",
            "asset": "BTCUSDT",
            "venue_type": "crypto",
            "log_ret": 0.0,
            "tr": 1.0,
            "atr": 1.0,
            "rv": 0.1,
        }
    )
    write_table(bars, dirs["bars"] / "BTCUSDT_15min.parquet")

    # Write intentionally bad pivots file (previous implementation would read this and could crash)
    bad_pivots = pd.DataFrame(
        {
            "asset": ["BTCUSDT", "BTCUSDT", "BTCUSDT"],
            "timeframe": ["15min", "15min", "15min"],
            "pivot_index": [9999, 10000, 10001],
            "ts_utc": [ts[0], ts[1], ts[2]],
            "pivot_type": ["HIGH", "LOW", "HIGH"],
            "pivot_price": [110.0, 90.0, 110.0],
        }
    )
    write_table(bad_pivots, dirs["turning_points"] / "BTCUSDT_15min_pivots.parquet")

    cfg = {
        "seed": 42,
        "assets": ["BTCUSDT"],
        "venues": {"BTCUSDT": "crypto"},
        "timeframes": ["15min"],
        "split": {"train_start": "2025-01-01", "train_end": "2025-01-01", "test_start": "2025-01-01", "test_end": "2025-01-02"},
        "paths": {"events_csv": str(tmp_path / "events.csv")},
        "features": {"regime_q": [0.33, 0.66], "atr_period": 14, "rv_window": 60},
        "turning_points": {"atr_lambda": 2.0, "min_separation_bars": 3},
        "outcomes": {"timeout_bars": 10, "atr_stop_margin": 0.5, "default_target_r": 1.5},
        "surrogates": {"n": 0, "stationary_bootstrap_block": 30},
        "detectors": {
            "hs": {"shoulder_tol": 0.08, "trough_tol": 0.10, "confirm_beta_atr": 0.2},
            "hs_ieee": {"enabled": False, "keypoint_window": 180, "min_prominence_atr": 0.7},
            "double": {"peak_tol": 0.04, "min_depth_atr": 1.0, "confirm_beta_atr": 0.2},
            "triangle": {"window_pivots": 8, "convergence_ratio": 0.7, "residual_eta_atr": 0.75, "confirm_beta_atr": 0.2},
            "symbolic": {"sax_window": 80, "paa_segments": 12, "alphabet_size": 7, "smoothness_weight": 0.3, "residual_weight": 0.7, "channel_threshold": 0.75},
        },
    }

    patterns, surr = _run_detection_layer(cfg, dirs, max_workers=1, progress_every=1)
    assert isinstance(patterns, pd.DataFrame)
    assert isinstance(surr, pd.DataFrame)
