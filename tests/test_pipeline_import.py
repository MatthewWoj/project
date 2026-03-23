from pathlib import Path

import pandas as pd

import patternfail.pipeline as pipeline
from patternfail.pipeline import _read_table, run_pipeline


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
