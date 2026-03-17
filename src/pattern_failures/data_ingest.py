from __future__ import annotations

from dataclasses import dataclass
from typing import Dict

import pandas as pd

from .schema import VenueType

CANONICAL_COLS = ["ts_utc", "open", "high", "low", "close", "volume", "asset", "venue_type"]


@dataclass
class DataQualityReport:
    duplicates: int
    conflicting_duplicates: int
    missing_minutes: int
    longest_gap_minutes: int
    stale_minutes: int


def ingest_csv(path: str, asset: str, venue_type: VenueType, dayfirst: bool = False) -> pd.DataFrame:
    df = pd.read_csv(path)
    col_map = {c.lower(): c for c in df.columns}
    ts_col = col_map.get("ts") or col_map.get("timestamp") or col_map.get("datetime")
    if not ts_col:
        raise ValueError("Expected one of ts/timestamp/datetime columns")

    df = df.rename(columns={ts_col: "ts_utc"})
    df["ts_utc"] = pd.to_datetime(df["ts_utc"], utc=True, dayfirst=dayfirst)
    for c in ["open", "high", "low", "close", "volume"]:
        df[c] = pd.to_numeric(df[c], errors="coerce")

    df["asset"] = asset
    df["venue_type"] = venue_type.value
    df = df[CANONICAL_COLS].dropna(subset=["ts_utc", "open", "high", "low", "close"])
    df = df.sort_values("ts_utc")
    return df


def deduplicate(df: pd.DataFrame) -> tuple[pd.DataFrame, int, int]:
    dup_mask = df.duplicated(["asset", "ts_utc"], keep=False)
    dups = df.loc[dup_mask].copy()
    conflicting = 0
    if not dups.empty:
        grouped = dups.groupby(["asset", "ts_utc"])
        for _, g in grouped:
            if g[["open", "high", "low", "close", "volume"]].nunique().max() > 1:
                conflicting += 1
    cleaned = df.drop_duplicates(["asset", "ts_utc"], keep="last").sort_values(["asset", "ts_utc"])
    return cleaned, int(dup_mask.sum()), conflicting


def detect_stale_minutes(df: pd.DataFrame, n: int = 10) -> pd.Series:
    flat = (df["open"] == df["high"]) & (df["high"] == df["low"]) & (df["low"] == df["close"]) & (df["volume"].fillna(0) == 0)
    groups = (flat != flat.shift(1)).cumsum()
    run_len = flat.groupby(groups).transform("size")
    return flat & (run_len >= n)


def missing_bar_diagnostics(df: pd.DataFrame) -> Dict[str, int]:
    idx = pd.DatetimeIndex(df["ts_utc"])
    full = pd.date_range(start=idx.min(), end=idx.max(), freq="min", tz="UTC")
    missing = full.difference(idx)
    diffs = idx.to_series().diff().dropna().dt.total_seconds().div(60)
    longest_gap = int(diffs.max()) if not diffs.empty else 0
    return {
        "missing_minutes": int(len(missing)),
        "longest_gap_minutes": longest_gap,
    }


def ingest_with_report(path: str, asset: str, venue_type: VenueType, dayfirst: bool = False, stale_n: int = 10) -> tuple[pd.DataFrame, DataQualityReport]:
    df = ingest_csv(path, asset=asset, venue_type=venue_type, dayfirst=dayfirst)
    df, duplicate_count, conflicting = deduplicate(df)
    stale = detect_stale_minutes(df, n=stale_n)
    miss = missing_bar_diagnostics(df)
    report = DataQualityReport(
        duplicates=duplicate_count,
        conflicting_duplicates=conflicting,
        missing_minutes=miss["missing_minutes"],
        longest_gap_minutes=miss["longest_gap_minutes"],
        stale_minutes=int(stale.sum()),
    )
    return df, report
