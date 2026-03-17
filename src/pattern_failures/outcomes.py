from __future__ import annotations

import pandas as pd

from .schema import Direction, PatternInstance, PatternType


def evaluate_pattern_outcome(instance: PatternInstance, bars: pd.DataFrame, timeout_bars: int = 50, atr_mult: float = 0.5) -> dict:
    idx = bars.index[bars["ts_utc"] == instance.t_confirm]
    if len(idx) == 0 or idx[0] + 1 >= len(bars):
        return {"status": "timeout"}
    i0 = int(idx[0]) + 1
    entry = float(bars.loc[i0, "open"])
    atr = float(bars.loc[i0, "atr"]) if pd.notna(bars.loc[i0, "atr"]) else 0.0

    if instance.pattern_type in (PatternType.HS_TOP, PatternType.DT) or instance.direction == Direction.SHORT:
        stop = entry + atr_mult * atr
        target = entry - atr
    else:
        stop = entry - atr_mult * atr
        target = entry + atr

    status = "timeout"
    exit_price = float(bars.loc[min(i0 + timeout_bars, len(bars) - 1), "close"])
    exit_idx = min(i0 + timeout_bars, len(bars) - 1)
    mfe, mae = 0.0, 0.0
    for j in range(i0, min(i0 + timeout_bars + 1, len(bars))):
        hi, lo = float(bars.loc[j, "high"]), float(bars.loc[j, "low"])
        if instance.direction == Direction.LONG:
            mfe = max(mfe, hi - entry)
            mae = min(mae, lo - entry)
            if lo <= stop:
                status, exit_price, exit_idx = "failure", stop, j
                break
            if hi >= target:
                status, exit_price, exit_idx = "success", target, j
                break
        else:
            mfe = max(mfe, entry - lo)
            mae = min(mae, entry - hi)
            if hi >= stop:
                status, exit_price, exit_idx = "failure", stop, j
                break
            if lo <= target:
                status, exit_price, exit_idx = "success", target, j
                break

    risk = abs(entry - stop) + 1e-12
    r = (exit_price - entry) / risk if instance.direction == Direction.LONG else (entry - exit_price) / risk
    return {
        "status": status,
        "entry": entry,
        "stop": stop,
        "target": target,
        "MFE": mfe,
        "MAE": mae,
        "R_multiple": r,
        "time_to_exit": int(exit_idx - i0),
    }
