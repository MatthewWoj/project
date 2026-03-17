from __future__ import annotations

from collections import defaultdict
from dataclasses import asdict

import numpy as np
import pandas as pd

from .significance import bh_fdr, effect_z, mc_pvalue


def summarize_instances(instances) -> pd.DataFrame:
    rows = []
    for p in instances:
        d = asdict(p)
        d["venue_type"] = p.venue_type.value
        d["pattern_type"] = p.pattern_type.value
        d["direction"] = p.direction.value
        rows.append(d)
    return pd.DataFrame(rows)


def existence_stats(instances_df: pd.DataFrame, bars_count: int) -> dict:
    if instances_df.empty:
        return {"T_count": 0.0, "T_strength": np.nan}
    return {
        "T_count": len(instances_df) / max(bars_count, 1) * 1_000_000,
        "T_strength": float(instances_df["score"].median()),
    }


def evaluate_significance(observed: dict, surrogates: list[dict]) -> dict:
    s_count = np.array([s["T_count"] for s in surrogates])
    s_strength = np.array([s["T_strength"] for s in surrogates])
    p_count = mc_pvalue(observed["T_count"], s_count, lower_tail=False)
    p_strength = mc_pvalue(observed["T_strength"], s_strength, lower_tail=True)
    q_count, q_strength = bh_fdr([p_count, p_strength])
    return {
        "p_count": p_count,
        "p_strength": p_strength,
        "q_count": q_count,
        "q_strength": q_strength,
        "z_count": effect_z(observed["T_count"], s_count),
        "z_strength": effect_z(observed["T_strength"], s_strength),
    }


def failure_by_context(instances_df: pd.DataFrame, context_col: str) -> pd.DataFrame:
    if instances_df.empty or "outcome_labels" not in instances_df:
        return pd.DataFrame()
    agg = defaultdict(lambda: {"n": 0, "fail": 0})
    for row in instances_df.itertuples(index=False):
        ctx = row.context_labels.get(context_col, "UNK")
        status = row.outcome_labels.get("status", "timeout") if row.outcome_labels else "timeout"
        agg[ctx]["n"] += 1
        agg[ctx]["fail"] += int(status == "failure")
    return pd.DataFrame([
        {"context": k, "n": v["n"], "failure_rate": v["fail"] / max(v["n"], 1)} for k, v in agg.items()
    ])
