from __future__ import annotations

import pandas as pd


def method_comparison(patterns: pd.DataFrame) -> pd.DataFrame:
    if patterns.empty:
        return patterns
    g = patterns.groupby(["detector_family", "detector_name"]).agg(n=("pattern_id", "size"), median_score=("score", "median")).reset_index()
    return g.sort_values(["detector_family", "n"], ascending=[True, False])
