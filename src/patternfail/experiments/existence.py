from __future__ import annotations

import numpy as np
import pandas as pd

from patternfail.stats.multiple_testing import benjamini_hochberg
from patternfail.stats.significance import effect_size_z, mc_pvalue


def existence_table(observed: pd.DataFrame, surrogate_stats: pd.DataFrame) -> pd.DataFrame:
    rows = []
    grouped = surrogate_stats.groupby(["asset", "timeframe", "pattern_type"])
    for key, obs_g in observed.groupby(["asset", "timeframe", "pattern_type"]):
        obs_count = len(obs_g)
        obs_strength = float(obs_g["score"].median())
        sims = grouped.get_group(key) if key in grouped.groups else pd.DataFrame(columns=["count_density", "strength"])
        c_sims = sims["count_density"].to_numpy() if not sims.empty else np.array([0.0])
        s_sims = sims["strength"].to_numpy() if not sims.empty else np.array([obs_strength])
        rows.append({
            "asset": key[0], "timeframe": key[1], "pattern_type": key[2],
            "T_count": obs_count, "T_strength": obs_strength,
            "p_count": mc_pvalue(obs_count, c_sims), "p_strength": mc_pvalue(obs_strength, s_sims, lower_tail=True),
            "z_count": effect_size_z(obs_count, c_sims), "z_strength": effect_size_z(obs_strength, s_sims),
        })
    out = pd.DataFrame(rows)
    if out.empty:
        return out
    out["q_count"] = benjamini_hochberg(out["p_count"].tolist())
    out["q_strength"] = benjamini_hochberg(out["p_strength"].tolist())
    return out
