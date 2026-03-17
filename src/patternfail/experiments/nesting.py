from __future__ import annotations

import pandas as pd


def apply_nesting(lower_tf: pd.DataFrame, higher_tf: pd.DataFrame) -> pd.DataFrame:
    out = lower_tf.copy()
    out["nested_in_pattern_id"] = None
    for i, r in out.iterrows():
        m = higher_tf[
            (higher_tf["asset"] == r["asset"]) &
            (higher_tf["t_start_utc"] <= r["t_start_utc"]) &
            (higher_tf["t_end_utc"] >= r["t_end_utc"])
        ]
        if not m.empty:
            out.at[i, "nested_in_pattern_id"] = m.iloc[0]["pattern_id"]
    return out


def nesting_summary(patterns: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for nested, g in patterns.groupby(patterns["nested_in_pattern_id"].notna()):
        rr = [((r["outcome_labels"] or {}).get("status") == "failure") for _, r in g.iterrows()]
        tte = [((r["outcome_labels"] or {}).get("time_to_exit", 0)) for _, r in g.iterrows()]
        rows.append({"nested": bool(nested), "n": len(g), "failure_rate": sum(rr) / max(len(rr), 1), "mean_time_to_exit": sum(tte) / max(len(tte), 1)})
    return pd.DataFrame(rows)
