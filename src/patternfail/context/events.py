from __future__ import annotations

from pathlib import Path

import pandas as pd


def load_events(path: str) -> pd.DataFrame:
    p = Path(path)
    if not p.exists():
        return pd.DataFrame(columns=["ts_utc", "event"])
    df = pd.read_csv(p)
    df["ts_utc"] = pd.to_datetime(df["ts_utc"], utc=True)
    return df


def label_event_window(ts: pd.Timestamp, events: pd.DataFrame, minutes: int = 30) -> str:
    if events.empty:
        return "NO_EVENT_FILE"
    delta = (events["ts_utc"] - ts).dt.total_seconds().abs() / 60
    if (delta <= minutes).any():
        return "DURING"
    if (delta <= 2 * minutes).any():
        return "PRE_POST"
    return "NONE"
