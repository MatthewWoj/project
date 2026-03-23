import pandas as pd

from patternfail.detectors.channels import detect_symbolic_channels
from patternfail.detectors.double_patterns import detect_double_patterns


def test_channel_geometry_declares_log_price_coordinate_system():
    ts = pd.date_range("2025-01-01", periods=120, freq="h", tz="UTC")
    close = pd.Series(range(100, 220), dtype=float)
    bars = pd.DataFrame(
        {
            "asset": "BTCUSDT",
            "venue_type": "crypto",
            "timeframe": "1h",
            "ts_utc": ts,
            "open": close,
            "high": close + 1.0,
            "low": close - 1.0,
            "close": close,
            "volume": 1.0,
        }
    )

    out = detect_symbolic_channels(
        bars,
        sax_window=40,
        paa_segments=8,
        alphabet_size=5,
        residual_weight=0.7,
        smoothness_weight=0.2,
        threshold=0.8,
        max_residual_ratio=0.8,
        min_r2=0.0,
    )

    assert not out.empty
    geom = out.iloc[0]["geometry_params"]
    assert geom["fitted_lines"]["center"]["coordinate_system"] == "log_price"
    assert geom["detection_status"] == "STRUCTURAL_ONLY"


def test_double_pattern_geometry_includes_pivots_and_neckline():
    bars = pd.DataFrame(
        {
            "asset": ["BTCUSDT"] * 6,
            "venue_type": ["crypto"] * 6,
            "timeframe": ["1h"] * 6,
            "ts_utc": pd.date_range("2025-01-01", periods=6, freq="h", tz="UTC"),
            "close": [100, 99, 101, 97, 100, 95],
            "atr": [1.0] * 6,
        }
    )
    pivots = pd.DataFrame(
        {
            "pivot_type": ["HIGH", "LOW", "HIGH"],
            "pivot_price": [100.0, 97.0, 100.5],
            "pivot_index": [0, 3, 4],
            "ts_utc": pd.date_range("2025-01-01", periods=3, freq="2h", tz="UTC"),
        }
    )

    out = detect_double_patterns(bars, pivots, peak_tol=0.02, min_depth_atr=1.0, beta_atr=0.0)

    assert not out.empty
    geom = out.iloc[0]["geometry_params"]
    assert len(geom["pivots_used"]) == 3
    assert geom["fitted_lines"]["neckline"]["kind"] == "horizontal"
