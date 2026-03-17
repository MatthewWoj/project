from __future__ import annotations

import numpy as np
import pandas as pd


def add_log_returns(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out["log_ret"] = np.log(out["close"]).diff()
    return out
