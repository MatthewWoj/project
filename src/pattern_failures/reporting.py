from __future__ import annotations

import pandas as pd


def diagnostics_table(reports: list[dict]) -> pd.DataFrame:
    return pd.DataFrame(reports)


def parameter_table(config) -> pd.DataFrame:
    rows = []
    for k, v in vars(config.thresholds).items():
        rows.append({"parameter": k, "value": v})
    return pd.DataFrame(rows)
