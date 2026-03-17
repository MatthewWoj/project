from __future__ import annotations

import pandas as pd


def failure_rate_by_context(patterns: pd.DataFrame, key: str) -> pd.DataFrame:
    rows = []
    for _, r in patterns.iterrows():
        c = (r["context_labels"] or {}).get(key, "UNK")
        s = (r["outcome_labels"] or {}).get("status", "timeout")
        rows.append({"context": c, "status": s})
    df = pd.DataFrame(rows)
    if df.empty:
        return df
    t = df.groupby("context").size().rename("n")
    f = df[df["status"] == "failure"].groupby("context").size().rename("f")
    out = pd.concat([t, f], axis=1).fillna(0).reset_index()
    out["failure_rate"] = out["f"] / out["n"]
    return out
