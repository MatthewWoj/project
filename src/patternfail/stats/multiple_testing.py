from __future__ import annotations

import numpy as np


def benjamini_hochberg(pvals: list[float]) -> list[float]:
    p = np.array(pvals); m = len(p)
    o = np.argsort(p)
    q = p[o] * m / (np.arange(m) + 1)
    q = np.minimum.accumulate(q[::-1])[::-1]
    out = np.empty(m); out[o] = q
    return out.tolist()
