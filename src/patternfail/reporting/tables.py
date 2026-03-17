from __future__ import annotations

import pandas as pd


def _flatten(prefix: str, value, rows: list[dict]) -> None:
    if isinstance(value, dict):
        for k, v in value.items():
            key = f"{prefix}.{k}" if prefix else k
            _flatten(key, v, rows)
    elif isinstance(value, list):
        rows.append({"parameter": prefix, "value": str(value)})
    else:
        rows.append({"parameter": prefix, "value": value})


def parameter_table(cfg: dict) -> pd.DataFrame:
    rows: list[dict] = []
    _flatten("", cfg, rows)
    return pd.DataFrame(rows)
