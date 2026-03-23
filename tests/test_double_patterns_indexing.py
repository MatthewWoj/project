import pandas as pd

from patternfail.detectors.double_patterns import detect_double_patterns


def test_double_patterns_uses_positional_indexing_for_pivots():
    # non-RangeIndex bars to emulate real-world filtered slices
    bars = pd.DataFrame(
        {
            "asset": ["BTCUSDT"] * 5,
            "venue_type": ["crypto"] * 5,
            "timeframe": ["1h"] * 5,
            "ts_utc": pd.date_range("2025-01-01", periods=5, freq="h", tz="UTC"),
            "close": [100, 98, 101, 97, 96],
            "atr": [1, 1, 1, 1, 1],
        },
        index=[10, 20, 30, 40, 50],
    )

    pivots = pd.DataFrame(
        {
            "asset": ["BTCUSDT", "BTCUSDT", "BTCUSDT"],
            "timeframe": ["1h", "1h", "1h"],
            "pivot_index": [0, 1, 2],
            "ts_utc": [bars.iloc[0]["ts_utc"], bars.iloc[1]["ts_utc"], bars.iloc[2]["ts_utc"]],
            "pivot_type": ["HIGH", "LOW", "HIGH"],
            "pivot_price": [100.0, 98.0, 100.0],
        }
    )

    out = detect_double_patterns(bars, pivots, peak_tol=0.05, min_depth_atr=1.0, beta_atr=0.2)
    assert isinstance(out, pd.DataFrame)
