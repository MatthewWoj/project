from __future__ import annotations

import numpy as np
import pandas as pd
from scipy.stats import norm


def sax_word(x: np.ndarray, m: int = 12, a: int = 7) -> str:
    if x.std() == 0:
        z = np.zeros_like(x)
    else:
        z = (x - x.mean()) / x.std()
    seg = np.array_split(z, m)
    paa = np.array([s.mean() for s in seg])
    cuts = norm.ppf(np.linspace(0, 1, a + 1)[1:-1])
    idx = np.searchsorted(cuts, paa)
    return "".join(chr(ord("a") + int(i)) for i in idx)


def cpc_diffs(word: str) -> np.ndarray:
    arr = np.array([ord(c) for c in word])
    return np.diff(arr)


def rolling_sax(df: pd.DataFrame, value_col: str = "log_ret", window: int = 80, m: int = 12, a: int = 7) -> pd.DataFrame:
    rows = []
    values = df[value_col].fillna(0).to_numpy()
    for i in range(window, len(values) + 1):
        seg = values[i - window : i]
        w = sax_word(seg, m=m, a=a)
        d = cpc_diffs(w)
        rows.append({"end_idx": i - 1, "word": w, "smoothness": float(np.mean(np.abs(d)))})
    return pd.DataFrame(rows)
