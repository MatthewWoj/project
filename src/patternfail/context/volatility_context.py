from __future__ import annotations

import pandas as pd


def label_pattern_vol_context(patterns: pd.DataFrame, bars: pd.DataFrame) -> pd.DataFrame:
    if patterns.empty:
        return patterns
    out = patterns.copy()
    for i, r in out.iterrows():
        hit = bars.index[bars["ts_utc"] == r["t_confirm_utc"]]
        vr = bars.loc[int(hit[0]), "vol_regime"] if len(hit) else "UNK"
        ctx = dict(r["context_labels"] or {})
        ctx["vol_regime"] = vr
        out.at[i, "context_labels"] = ctx
    return out
