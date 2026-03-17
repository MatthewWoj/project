from __future__ import annotations

import numpy as np
import pandas as pd


def permutation_surrogate(close: pd.Series, rng: np.random.Generator | None = None) -> pd.Series:
    rng = rng or np.random.default_rng()
    ret = np.log(close).diff().fillna(0).to_numpy()
    perm = rng.permutation(ret)
    out = np.exp(np.cumsum(perm) + np.log(close.iloc[0]))
    return pd.Series(out, index=close.index)


def stationary_bootstrap_surrogate(close: pd.Series, expected_block: int = 30, rng: np.random.Generator | None = None) -> pd.Series:
    rng = rng or np.random.default_rng()
    ret = np.log(close).diff().fillna(0).to_numpy()
    n = len(ret)
    p = 1 / expected_block
    sample = np.empty(n)
    idx = rng.integers(0, n)
    for i in range(n):
        sample[i] = ret[idx]
        if rng.random() < p:
            idx = rng.integers(0, n)
        else:
            idx = (idx + 1) % n
    out = np.exp(np.cumsum(sample) + np.log(close.iloc[0]))
    return pd.Series(out, index=close.index)
