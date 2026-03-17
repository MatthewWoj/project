from __future__ import annotations

import numpy as np


def mc_pvalue(observed: float, surrogates: np.ndarray, lower_tail: bool = False) -> float:
    if lower_tail:
        b = int(np.sum(surrogates <= observed))
    else:
        b = int(np.sum(surrogates >= observed))
    return (1 + b) / (len(surrogates) + 1)


def effect_z(observed: float, surrogates: np.ndarray) -> float:
    return float((observed - np.mean(surrogates)) / (np.std(surrogates) + 1e-12))


def bh_fdr(pvals: list[float]) -> list[float]:
    m = len(pvals)
    order = np.argsort(pvals)
    ranked = np.array(pvals)[order]
    q = ranked * m / (np.arange(m) + 1)
    q = np.minimum.accumulate(q[::-1])[::-1]
    out = np.empty(m)
    out[order] = q
    return out.tolist()
