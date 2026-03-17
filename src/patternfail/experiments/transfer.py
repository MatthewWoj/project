from __future__ import annotations

import pandas as pd


def transfer_summary(patterns: pd.DataFrame, base_timeframe: str) -> pd.DataFrame:
    base = patterns[patterns["timeframe"] == base_timeframe]
    b = base.groupby("pattern_type")["score"].median().rename("base_median_score")
    o = patterns.groupby(["timeframe", "pattern_type"]).agg(count=("pattern_id", "size"), median_score=("score", "median")).reset_index()
    o = o.merge(b, on="pattern_type", how="left")
    o["score_shift_vs_base"] = o["median_score"] - o["base_median_score"]
    return o
