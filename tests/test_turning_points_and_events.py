from pathlib import Path

import pandas as pd

from patternfail.context.events import label_event_context, load_events
from patternfail.turning_points.extrema import extract_pivots_smoothed_extrema


def test_smoothed_extrema_extractor_returns_pivot_method():
    ts = pd.date_range("2025-01-01", periods=11, freq="min", tz="UTC")
    close = [1.0, 2.0, 4.0, 2.5, 1.5, 1.0, 1.8, 3.8, 2.0, 1.0, 0.5]
    bars = pd.DataFrame(
        {
            "asset": "BTCUSDT",
            "timeframe": "1min",
            "ts_utc": ts,
            "close": close,
            "atr": [0.5] * len(ts),
        }
    )

    pivots = extract_pivots_smoothed_extrema(bars, smoothing_window=3, prominence_atr=0.5, min_sep=1)

    assert not pivots.empty
    assert set(pivots["pivot_method"]) == {"smoothed_extrema"}


def test_event_context_labels_pre_and_post(tmp_path: Path):
    path = tmp_path / "events.csv"
    path.write_text(
        "ts_utc,event_name,event_type,country,currency\n"
        "2025-01-01T12:00:00Z,CPI,macro,US,USD\n",
        encoding="utf-8",
    )

    events = load_events(str(path))

    pre = label_event_context(pd.Timestamp("2025-01-01T11:45:00Z"), events, pre_minutes=30, post_minutes=30)
    post = label_event_context(pd.Timestamp("2025-01-01T12:15:00Z"), events, pre_minutes=30, post_minutes=30)

    assert pre["event_window"] == "PRE"
    assert pre["event_type"] == "macro"
    assert post["event_window"] == "POST"
