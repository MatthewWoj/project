from pathlib import Path

import pandas as pd

import patternfail.pipeline as pipeline
from patternfail.pipeline import _detect_patterns, _read_table, run_pipeline


def test_pipeline_module_imports() -> None:
    assert callable(run_pipeline)


def test_read_table_falls_back_to_csv_when_parquet_engine_missing(tmp_path: Path, monkeypatch) -> None:
    parquet_path = tmp_path / "bars.parquet"
    parquet_path.write_text("placeholder", encoding="utf-8")
    csv_path = tmp_path / "bars.csv"
    pd.DataFrame({"x": [1, 2]}).to_csv(csv_path, index=False)

    def boom(_path):
        raise ImportError("missing parquet engine")

    monkeypatch.setattr(pipeline.pd, "read_parquet", boom)

    out = _read_table(parquet_path)

    assert out["x"].tolist() == [1, 2]


def test_pipeline_module_exposes_deduplicate_helper() -> None:
    assert hasattr(pipeline, "_deduplicate_patterns")
    assert callable(pipeline._deduplicate_patterns)


def test_detect_patterns_skips_dedup_if_helper_missing(monkeypatch) -> None:
    calls: list[str] = []

    def fake_detector(*_args, **_kwargs):
        calls.append("detector")
        return pd.DataFrame(
            [
                {
                    "pattern_id": "p1",
                    "asset": "BTCUSDT",
                    "timeframe": "1h",
                    "pattern_type": "HS_TOP",
                    "direction": "SHORT",
                    "detector_name": "hs_lo",
                    "t_start_utc": "2025-01-01T00:00:00Z",
                    "t_end_utc": "2025-01-01T01:00:00Z",
                    "t_confirm_utc": "2025-01-01T02:00:00Z",
                    "score": 0.1,
                }
            ]
        )

    monkeypatch.setattr(pipeline, "detect_hs_lo", fake_detector)
    monkeypatch.setattr(pipeline, "detect_double_patterns", lambda *_args, **_kwargs: pd.DataFrame())
    monkeypatch.setattr(pipeline, "detect_triangles", lambda *_args, **_kwargs: pd.DataFrame())
    monkeypatch.setattr(pipeline, "detect_symbolic_channels", lambda *_args, **_kwargs: pd.DataFrame())
    monkeypatch.setattr(pipeline, "detect_hs_ieee_comparator", lambda *_args, **_kwargs: pd.DataFrame())
    monkeypatch.delattr(pipeline, "_deduplicate_patterns")

    cfg = {
        "detectors": {
            "hs": {"shoulder_tol": 0.08, "trough_tol": 0.10, "confirm_beta_atr": 0.2},
            "double": {"peak_tol": 0.04, "min_depth_atr": 1.0, "confirm_beta_atr": 0.2},
            "triangle": {"window_pivots": 8, "convergence_ratio": 0.7, "residual_eta_atr": 0.75, "confirm_beta_atr": 0.2},
            "symbolic": {"sax_window": 80, "paa_segments": 12, "alphabet_size": 7, "residual_weight": 0.7, "smoothness_weight": 0.3, "channel_threshold": 0.75},
            "hs_ieee": {"enabled": False, "keypoint_window": 180, "min_prominence_atr": 0.7},
        }
    }

    out = _detect_patterns(pd.DataFrame(), cfg, pd.DataFrame())

    assert calls == ["detector"]
    assert out["pattern_id"].tolist() == ["p1"]
