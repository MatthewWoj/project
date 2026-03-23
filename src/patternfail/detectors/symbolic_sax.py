from __future__ import annotations

import numpy as np
from scipy.stats import norm


def sax_word(values: np.ndarray, segments: int, alphabet: int) -> str:
    z = np.zeros_like(values) if values.std() == 0 else (values - values.mean()) / values.std()
    paa = np.array([x.mean() for x in np.array_split(z, segments)])
    cuts = norm.ppf(np.linspace(0, 1, alphabet + 1)[1:-1])
    idx = np.searchsorted(cuts, paa)
    return "".join(chr(ord("a") + int(i)) for i in idx)


def smoothness(word: str) -> float:
    vals = np.array([ord(c) for c in word])
    d = np.diff(vals)
    return float(np.mean(np.abs(d))) if len(d) else 0.0


def ascii_differences(word: str) -> list[int]:
    vals = np.array([ord(c) for c in word], dtype=int)
    if len(vals) <= 1:
        return []
    return np.diff(vals).astype(int).tolist()
