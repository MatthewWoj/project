from __future__ import annotations

import pandas as pd


def label_outcomes(patterns: pd.DataFrame, bars: pd.DataFrame, timeout_bars: int = 60, atr_stop_margin: float = 0.5, default_target_r: float = 1.5) -> pd.DataFrame:
    if patterns.empty:
        return patterns
    out = patterns.copy()
    out["outcome_labels"] = None
    for idx, r in out.iterrows():
        hit = bars.index[bars["ts_utc"] == r["t_confirm_utc"]]
        if len(hit) == 0 or int(hit[0]) + 1 >= len(bars):
            out.at[idx, "outcome_labels"] = {"status": "timeout"}
            continue
        i0 = int(hit[0]) + 1
        entry = float(bars.loc[i0, "open"])
        atr = float(bars.loc[i0, "atr"]) if pd.notna(bars.loc[i0, "atr"]) else 0.0
        long = r["direction"] == "LONG"
        stop = entry - atr_stop_margin * atr if long else entry + atr_stop_margin * atr
        risk = abs(entry - stop) + 1e-12
        target = entry + default_target_r * risk if long else entry - default_target_r * risk

        status = "timeout"; ambiguity = False; exit_price = float(bars.loc[min(i0 + timeout_bars, len(bars)-1), "close"]); exit_i = min(i0 + timeout_bars, len(bars)-1)
        mfe = 0.0; mae = 0.0
        for j in range(i0, min(i0 + timeout_bars + 1, len(bars))):
            hi = float(bars.loc[j, "high"]); lo = float(bars.loc[j, "low"])
            if long:
                stop_hit, target_hit = lo <= stop, hi >= target
                mfe = max(mfe, hi - entry); mae = min(mae, lo - entry)
            else:
                stop_hit, target_hit = hi >= stop, lo <= target
                mfe = max(mfe, entry - lo); mae = min(mae, entry - hi)
            if stop_hit and target_hit:
                ambiguity = True
            if stop_hit:
                status, exit_price, exit_i = "failure", stop, j; break
            if target_hit:
                status, exit_price, exit_i = "success", target, j; break

        r_mult = (exit_price - entry) / risk if long else (entry - exit_price) / risk
        out.at[idx, "outcome_labels"] = {
            "status": status, "entry": entry, "stop": stop, "target": target, "MFE": mfe, "MAE": mae,
            "R_multiple": float(r_mult), "time_to_exit": int(exit_i - i0), "exit_reason": status, "bar_ambiguity": ambiguity,
        }
    return out
