from pathlib import Path

import pandas as pd

from patternfail.data.bars import aggregate_bars
from patternfail.data.ingest import ingest_asset_csv


def test_ingest_asset_override_mapping_and_tz(tmp_path: Path):
    p = tmp_path / "eur.csv"
    p.write_text("DateTime,Open,High,Low,Close,TickVolume\n2025-01-01 09:00:00,1,2,0.5,1.5,10\n", encoding="utf-8")
    cfg = {
        "timestamp_col": "timestamp",
        "assume_tz": "UTC",
        "cols": {"open": "open", "high": "high", "low": "low", "close": "close", "volume": "volume"},
        "asset_overrides": {
            "EURUSD": {
                "timestamp_col": "DateTime",
                "assume_tz": "Europe/London",
                "cols": {"open": "Open", "high": "High", "low": "Low", "close": "Close", "volume": "TickVolume"},
            }
        },
    }

    df, _ = ingest_asset_csv("EURUSD", "fx", str(p), cfg)
    assert list(df.columns) == ["asset", "venue_type", "ts_utc", "open", "high", "low", "close", "volume"]
    assert str(df.loc[0, "ts_utc"].tz) == "UTC"


def test_reconstructed_timeframes_exist_for_crypto():
    idx = pd.date_range("2025-01-01", periods=60 * 24 * 8, freq="min", tz="UTC")
    df = pd.DataFrame(
        {
            "asset": "BTCUSDT",
            "venue_type": "crypto",
            "ts_utc": idx,
            "open": 1.0,
            "high": 2.0,
            "low": 0.5,
            "close": 1.5,
            "volume": 1.0,
        }
    )
    timeframes = ["3min", "5min", "15min", "1h", "4h", "1d", "1w"]
    out = {tf: aggregate_bars(df, tf, "crypto") for tf in timeframes}
    assert all(not out[tf].empty for tf in timeframes)
