from __future__ import annotations

import importlib.util
from pathlib import Path

import pandas as pd
import pytest


def _load_review_detection_module():
    module_path = Path(__file__).resolve().parents[1] / "scripts" / "review_detection.py"
    spec = importlib.util.spec_from_file_location("review_detection", module_path)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


review_detection = _load_review_detection_module()


def test_select_pattern_raises_clear_error_for_missing_pattern_id():
    patterns = pd.DataFrame(
        [
            {"pattern_id": "abc123", "pattern_type": "HS_TOP"},
            {"pattern_id": "def456", "pattern_type": "CHANNEL"},
        ]
    )

    with pytest.raises(ValueError, match="Pattern id 'missing-id' was not found") as excinfo:
        review_detection._select_pattern(patterns, pattern_id="missing-id", pattern_type=None)

    assert "abc123" in str(excinfo.value)
    assert "def456" in str(excinfo.value)


def test_select_pattern_raises_clear_error_for_empty_asset_timeframe_slice():
    patterns = pd.DataFrame(columns=["pattern_id", "pattern_type"])

    with pytest.raises(ValueError, match="No patterns found for the selected asset/timeframe"):
        review_detection._select_pattern(patterns, pattern_id=None, pattern_type=None)


def test_line_values_falls_back_to_legacy_geom_when_fitted_line_is_incomplete():
    bars = pd.DataFrame(
        {
            "ts_utc": pd.to_datetime([
                "2025-01-01T00:00:00Z",
                "2025-01-01T01:00:00Z",
                "2025-01-01T02:00:00Z",
            ], utc=True),
            "low": [99.0, 100.0, 101.0],
            "high": [101.0, 102.0, 103.0],
        },
        index=[10, 11, 12],
    )
    geom = {"neckline_slope": 2.0, "neckline_intercept": 5.0}

    values = review_detection._line_values({"kind": "affine", "line_key": "neckline"}, bars, geom)

    assert values.tolist() == [25.0, 27.0, 29.0]


def test_line_values_returns_none_when_line_definition_is_missing():
    bars = pd.DataFrame(
        {
            "ts_utc": pd.to_datetime(["2025-01-01T00:00:00Z"], utc=True),
            "low": [99.0],
            "high": [101.0],
        },
        index=[0],
    )

    assert review_detection._line_values({"kind": "affine", "line_key": "neckline"}, bars, geom={}) is None


def test_plot_line_uses_line_key_to_fall_back_to_legacy_geom():
    bars = pd.DataFrame(
        {
            "ts_utc": pd.to_datetime([
                "2025-01-01T00:00:00Z",
                "2025-01-01T01:00:00Z",
            ], utc=True),
            "low": [99.0, 100.0],
            "high": [101.0, 102.0],
        },
        index=[0, 1],
    )
    geom = {"neckline_slope": 1.0, "neckline_intercept": 100.0}

    class StubAxis:
        def __init__(self):
            self.calls = []

        def plot(self, *args, **kwargs):
            self.calls.append((args, kwargs))

    ax = StubAxis()

    plotted = review_detection._plot_line(ax, bars, geom, {"kind": "affine"}, "neckline", "--", line_key="neckline")

    assert plotted is True
    assert len(ax.calls) == 1
