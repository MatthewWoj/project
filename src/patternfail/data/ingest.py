from __future__ import annotations

import logging
from dataclasses import dataclass
from pathlib import Path

import pandas as pd

logger = logging.getLogger(__name__)
CANONICAL = ["asset", "venue_type", "ts_utc", "open", "high", "low", "close", "volume"]
PLACEHOLDER_TOKEN = "/REPLACE/WITH/YOUR/PATH/"


@dataclass
class IngestReport:
    duplicate_count: int
    invalid_ohlc_count: int


def _validate_csv_path(asset: str, csv_path: str) -> Path:
    raw_path = str(csv_path).strip()
    if not raw_path:
        raise FileNotFoundError(
            f"Input CSV path for asset '{asset}' is empty. Set input_csv.{asset} in your config before running the pipeline."
        )
    if PLACEHOLDER_TOKEN in raw_path:
        raise FileNotFoundError(
            "Input CSV path for asset "
            f"'{asset}' still uses the example placeholder: {raw_path}\n"
            f"Open your config file and replace input_csv.{asset} with the real file path.\n"
            "On Windows you can run: notepad configs/my_run.yaml"
        )

    path = Path(raw_path).expanduser()
    if not path.exists():
        raise FileNotFoundError(
            f"Input CSV path for asset '{asset}' does not exist: {path}\n"
            f"Update input_csv.{asset} in your config to point at the real file."
        )
    if path.is_dir():
        raise FileNotFoundError(
            f"Input CSV path for asset '{asset}' points to a directory, not a file: {path}\n"
            f"Update input_csv.{asset} to the CSV file itself."
        )
    return path


def ingest_asset_csv(asset: str, venue_type: str, csv_path: str, csv_cfg: dict) -> tuple[pd.DataFrame, IngestReport]:
    override = csv_cfg.get("asset_overrides", {}).get(asset, {})
    c = override.get("cols", csv_cfg["cols"])
    ts_col = override.get("timestamp_col", csv_cfg["timestamp_col"])
    day_first = bool(override.get("day_first", csv_cfg.get("day_first", False)))
    assume_tz = override.get("assume_tz", csv_cfg.get("assume_tz", "UTC"))
    sep = override.get("separator", csv_cfg.get("separator", ","))
    csv_file = _validate_csv_path(asset, csv_path)

    df = pd.read_csv(csv_file, sep=sep)
    rename = {
        ts_col: "ts_utc",
        c["open"]: "open",
        c["high"]: "high",
        c["low"]: "low",
        c["close"]: "close",
        c["volume"]: "volume",
    }
    df = df.rename(columns=rename)
    missing = [k for k in rename.values() if k not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns in {asset}: {missing}")

    ts = pd.to_datetime(df["ts_utc"], errors="coerce", dayfirst=day_first)
    if ts.dt.tz is None:
        ts = ts.dt.tz_localize(assume_tz, ambiguous="infer", nonexistent="shift_forward")
    df["ts_utc"] = ts.dt.tz_convert("UTC")

    for col in ["open", "high", "low", "close", "volume"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    invalid = (~((df["low"] <= df["open"]) & (df["low"] <= df["close"]) & (df["high"] >= df["open"]) & (df["high"] >= df["close"])) | df[["open", "high", "low", "close"]].isna().any(axis=1))
    invalid_count = int(invalid.sum())
    df = df.loc[~invalid].copy()

    dup_mask = df.duplicated(subset=["ts_utc"], keep="last")
    dup_count = int(dup_mask.sum())
    df = df.loc[~dup_mask].copy()

    df["asset"] = asset
    df["venue_type"] = venue_type
    df = df[CANONICAL].sort_values("ts_utc").reset_index(drop=True)

    logger.info("ingested %s rows for %s", len(df), asset)
    return df, IngestReport(duplicate_count=dup_count, invalid_ohlc_count=invalid_count)
