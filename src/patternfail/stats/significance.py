from __future__ import annotations

import numpy as np


def mc_pvalue(obs: float, sims: np.ndarray, lower_tail: bool = False) -> float:
    b = int(np.sum(sims <= obs)) if lower_tail else int(np.sum(sims >= obs))
    return (1 + b) / (len(sims) + 1)


def effect_size_z(obs: float, sims: np.ndarray) -> float:
    return float((obs - sims.mean()) / (sims.std() + 1e-12))
