from __future__ import annotations

from pathlib import Path

import pandas as pd


def load_events(path: str) -> pd.DataFrame:
    p = Path(path)
    if not p.exists():
        return pd.DataFrame(columns=["ts_utc", "event_name", "event_type", "country", "currency"])
    df = pd.read_csv(p)
    rename = {
        "event": "event_name",
        "name": "event_name",
        "type": "event_type",
    }
    df = df.rename(columns={k: v for k, v in rename.items() if k in df.columns})
    df["ts_utc"] = pd.to_datetime(df["ts_utc"], utc=True)
    for col in ["event_name", "event_type", "country", "currency"]:
        if col not in df.columns:
            df[col] = ""
    return df


def label_event_context(ts: pd.Timestamp, events: pd.DataFrame, pre_minutes: int = 30, post_minutes: int = 30) -> dict:
    if events.empty:
        return {
            "event_window": "NO_EVENT_FILE",
            "event_name": None,
            "event_type": None,
            "event_country": None,
            "event_currency": None,
            "event_distance_min": None,
        }

    deltas = (events["ts_utc"] - ts).dt.total_seconds() / 60.0
    nearest_idx = deltas.abs().idxmin()
    nearest = events.loc[nearest_idx]
    nearest_delta = float(deltas.loc[nearest_idx])
    if abs(nearest_delta) <= 1e-9:
        window = "DURING"
    elif 0 < nearest_delta <= pre_minutes:
        window = "PRE"
    elif -post_minutes <= nearest_delta < 0:
        window = "POST"
    else:
        window = "NONE"

    return {
        "event_window": window,
        "event_name": nearest.get("event_name") or None,
        "event_type": nearest.get("event_type") or None,
        "event_country": nearest.get("country") or None,
        "event_currency": nearest.get("currency") or None,
        "event_distance_min": round(nearest_delta, 3) if window != "NONE" else None,
    }


def label_event_window(ts: pd.Timestamp, events: pd.DataFrame, minutes: int = 30) -> str:
    return label_event_context(ts, events, pre_minutes=minutes, post_minutes=minutes)["event_window"]
