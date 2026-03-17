from __future__ import annotations

import logging
from pathlib import Path

import pandas as pd


logger = logging.getLogger(__name__)


def write_table(df: pd.DataFrame, path: Path) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.suffix == ".parquet":
        try:
            df.to_parquet(path, index=False)
            return path
        except Exception as exc:
            logger.warning("Parquet unavailable (%s), fallback to csv", exc)
            alt = path.with_suffix(".csv")
            df.to_csv(alt, index=False)
            return alt
    if path.suffix == ".csv":
        df.to_csv(path, index=False)
        return path
    if path.suffix == ".pkl":
        df.to_pickle(path)
        return path
    df.to_csv(path.with_suffix(".csv"), index=False)
    return path.with_suffix(".csv")
