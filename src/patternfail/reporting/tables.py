from __future__ import annotations

import pandas as pd


def parameter_table(cfg: dict) -> pd.DataFrame:
    rows = []
    for k, v in cfg.items():
        if isinstance(v, (dict, list)):
            continue
        rows.append({"parameter": k, "value": v})
    return pd.DataFrame(rows)
