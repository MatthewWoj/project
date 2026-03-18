from __future__ import annotations

import json

import pandas as pd


def _as_mapping(value):
    if isinstance(value, dict):
        return value
    if isinstance(value, str):
        value = value.strip()
        if value.startswith("{"):
            try:
                parsed = json.loads(value)
            except json.JSONDecodeError:
                return {}
            return parsed if isinstance(parsed, dict) else {}
    return {}


def failure_rate_by_context(patterns: pd.DataFrame, key: str) -> pd.DataFrame:
    rows = []
    for _, r in patterns.iterrows():
        context_labels = _as_mapping(r["context_labels"])
        outcome_labels = _as_mapping(r["outcome_labels"])
        c = context_labels.get(key, "UNK")
        s = outcome_labels.get("status", "timeout")
        rows.append({"context": c, "status": s})
    df = pd.DataFrame(rows)
    if df.empty:
        return df
    t = df.groupby("context").size().rename("n")
    f = df[df["status"] == "failure"].groupby("context").size().rename("f")
    out = pd.concat([t, f], axis=1).fillna(0).reset_index()
    out["failure_rate"] = out["f"] / out["n"]
    return out
