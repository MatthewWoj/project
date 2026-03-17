from __future__ import annotations

import numpy as np
import pandas as pd


def returns_permutation(close: pd.Series, rng: np.random.Generator) -> pd.Series:
    r = np.log(close).diff().fillna(0).to_numpy()
    rp = rng.permutation(r)
    return pd.Series(np.exp(np.cumsum(rp) + np.log(close.iloc[0])), index=close.index)


def stationary_bootstrap(close: pd.Series, expected_block: int, rng: np.random.Generator) -> pd.Series:
    r = np.log(close).diff().fillna(0).to_numpy(); n = len(r)
    p = 1.0 / expected_block
    out = np.empty(n); i = int(rng.integers(0, n))
    for k in range(n):
        out[k] = r[i]
        i = int(rng.integers(0, n)) if rng.random() < p else (i + 1) % n
    return pd.Series(np.exp(np.cumsum(out) + np.log(close.iloc[0])), index=close.index)
