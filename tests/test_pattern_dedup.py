import pandas as pd

from patternfail.pipeline import _deduplicate_patterns


def _cfg(enabled=True):
    return {"dedup": {"enabled": enabled, "overlap_threshold": 0.8, "confirm_within_bars": 3}}


def test_deduplicate_patterns_removes_same_type_overlaps():
    patterns = pd.DataFrame([
        {"pattern_id": "a", "asset": "BTCUSDT", "timeframe": "1h", "pattern_type": "HS_TOP", "direction": "SHORT", "detector_name": "hs_lo", "t_start_utc": "2025-01-01T00:00:00Z", "t_end_utc": "2025-01-01T10:00:00Z", "t_confirm_utc": "2025-01-01T11:00:00Z", "score": 0.10},
        {"pattern_id": "b", "asset": "BTCUSDT", "timeframe": "1h", "pattern_type": "HS_TOP", "direction": "SHORT", "detector_name": "hs_lo", "t_start_utc": "2025-01-01T00:30:00Z", "t_end_utc": "2025-01-01T10:30:00Z", "t_confirm_utc": "2025-01-01T11:00:00Z", "score": 0.20},
    ])
    out = _deduplicate_patterns(patterns, _cfg())
    assert list(out["pattern_id"]) == ["a"]


def test_deduplicate_patterns_keeps_different_pattern_types():
    patterns = pd.DataFrame([
        {"pattern_id": "a", "asset": "BTCUSDT", "timeframe": "1h", "pattern_type": "HS_TOP", "direction": "SHORT", "detector_name": "hs_lo", "t_start_utc": "2025-01-01T00:00:00Z", "t_end_utc": "2025-01-01T10:00:00Z", "t_confirm_utc": "2025-01-01T11:00:00Z", "score": 0.10},
        {"pattern_id": "b", "asset": "BTCUSDT", "timeframe": "1h", "pattern_type": "TRIANGLE_SYM", "direction": "SHORT", "detector_name": "triangles", "t_start_utc": "2025-01-01T00:00:00Z", "t_end_utc": "2025-01-01T10:00:00Z", "t_confirm_utc": "2025-01-01T11:00:00Z", "score": 0.20},
    ])
    out = _deduplicate_patterns(patterns, _cfg())
    assert set(out["pattern_id"]) == {"a", "b"}


def test_deduplicate_patterns_keeps_same_type_if_confirmations_are_far_apart():
    patterns = pd.DataFrame([
        {"pattern_id": "a", "asset": "BTCUSDT", "timeframe": "1h", "pattern_type": "CHANNEL", "direction": "LONG", "detector_name": "sax_channel", "t_start_utc": "2025-01-01T00:00:00Z", "t_end_utc": "2025-01-01T08:00:00Z", "t_confirm_utc": "2025-01-01T08:00:00Z", "score": 0.05},
        {"pattern_id": "b", "asset": "BTCUSDT", "timeframe": "1h", "pattern_type": "CHANNEL", "direction": "LONG", "detector_name": "sax_channel", "t_start_utc": "2025-01-01T00:15:00Z", "t_end_utc": "2025-01-01T08:15:00Z", "t_confirm_utc": "2025-01-01T16:00:00Z", "score": 0.06},
    ])
    out = _deduplicate_patterns(patterns, _cfg())
    assert set(out["pattern_id"]) == {"a", "b"}
