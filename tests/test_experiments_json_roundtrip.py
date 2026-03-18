from pathlib import Path

import pandas as pd

from patternfail.common.paths import ensure_run_dirs
from patternfail.pipeline import _deserialize_json_cols, _run_experiments_layer


def test_deserialize_json_cols_parses_serialized_dict_columns():
    df = pd.DataFrame(
        {
            "geometry_params": ['{"width": 1.5}'],
            "context_labels": ['{"session_bucket": "us_rth"}'],
            "outcome_labels": ['{"status": "failure", "time_to_exit": 4}'],
        }
    )

    out = _deserialize_json_cols(df)

    assert out.loc[0, "geometry_params"] == {"width": 1.5}
    assert out.loc[0, "context_labels"] == {"session_bucket": "us_rth"}
    assert out.loc[0, "outcome_labels"] == {"status": "failure", "time_to_exit": 4}


def test_run_experiments_layer_reads_csv_pattern_files_with_json_strings(tmp_path: Path):
    dirs = ensure_run_dirs(str(tmp_path), "run")
    patterns = pd.DataFrame(
        {
            "pattern_id": ["p1"],
            "asset": ["BTCUSDT"],
            "venue_type": ["crypto"],
            "timeframe": ["1h"],
            "pattern_type": ["DT"],
            "direction": ["SHORT"],
            "t_start_utc": ["2025-01-01T00:00:00Z"],
            "t_end_utc": ["2025-01-01T01:00:00Z"],
            "t_confirm_utc": ["2025-01-01T02:00:00Z"],
            "score": [0.1],
            "geometry_params": ['{"similarity": 0.1}'],
            "detector_family": ["geometric"],
            "detector_name": ["double_patterns"],
            "context_labels": ['{"session_bucket": "asia", "vol_regime": "high"}'],
            "nested_in_pattern_id": [None],
            "outcome_labels": ['{"status": "failure", "time_to_exit": 3}'],
        }
    )
    patterns.to_csv(dirs["patterns"] / "BTCUSDT_1h_patterns.csv", index=False)

    cfg = {
        "assets": ["BTCUSDT"],
        "timeframes": ["1h"],
        "experiments": {"base_timeframe": "1h"},
    }

    _run_experiments_layer(cfg, dirs)

    failure_by_session = pd.read_csv(dirs["experiments"] / "failure_by_session.csv")
    assert failure_by_session.loc[0, "context"] == "asia"
    assert failure_by_session.loc[0, "failure_rate"] == 1.0
