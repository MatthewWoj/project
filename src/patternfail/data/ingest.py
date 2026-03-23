from __future__ import annotations

import logging
from dataclasses import dataclass

import pandas as pd

logger = logging.getLogger(__name__)
CANONICAL = ["asset", "venue_type", "ts_utc", "open", "high", "low", "close", "volume"]


@dataclass
class IngestReport:
    duplicate_count: int
    invalid_ohlc_count: int


def ingest_asset_csv(asset: str, venue_type: str, csv_path: str, csv_cfg: dict) -> tuple[pd.DataFrame, IngestReport]:
    override = csv_cfg.get("asset_overrides", {}).get(asset, {})
    c = override.get("cols", csv_cfg["cols"])
    ts_col = override.get("timestamp_col", csv_cfg["timestamp_col"])
    day_first = bool(override.get("day_first", csv_cfg.get("day_first", False)))
    assume_tz = override.get("assume_tz", csv_cfg.get("assume_tz", "UTC"))
    sep = override.get("separator", csv_cfg.get("separator", ","))

    df = pd.read_csv(csv_path, sep=sep)
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
