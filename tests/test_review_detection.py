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
