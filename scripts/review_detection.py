from __future__ import annotations

import argparse
import json
from pathlib import Path

import matplotlib.dates as mdates
import matplotlib.pyplot as plt
import pandas as pd


def _read_table(path: Path) -> pd.DataFrame:
    if path.suffix == ".parquet" and path.exists():
        return pd.read_parquet(path)
    alt = path.with_suffix(".csv")
    if alt.exists():
        return pd.read_csv(alt)
    raise FileNotFoundError(path)


def _to_dict(v):
    if isinstance(v, dict):
        return v
    if isinstance(v, str) and v.startswith("{"):
        return json.loads(v)
    return {}


def plot_detection(run_root: Path, asset: str, timeframe: str, pattern_id: str | None, pattern_type: str | None, window_bars: int, show_outcome: bool, save_path: Path | None):
    bars = _read_table(run_root / "bars" / f"{asset}_{timeframe}.parquet")
    patterns = _read_table(run_root / "patterns" / f"{asset}_{timeframe}_patterns.parquet")
    bars["ts_utc"] = pd.to_datetime(bars["ts_utc"], utc=True)
    patterns["t_start_utc"] = pd.to_datetime(patterns["t_start_utc"], utc=True)
    patterns["t_confirm_utc"] = pd.to_datetime(patterns["t_confirm_utc"], utc=True)

    if pattern_id:
        pat = patterns[patterns["pattern_id"] == pattern_id].iloc[0]
    elif pattern_type:
        pat = patterns[patterns["pattern_type"] == pattern_type].iloc[0]
    else:
        pat = patterns.iloc[0]

    i_confirm = bars.index[bars["ts_utc"] >= pat["t_confirm_utc"]][0]
    lo = max(i_confirm - window_bars, 0)
    hi = min(i_confirm + (window_bars if show_outcome else 10), len(bars) - 1)
    b = bars.iloc[lo : hi + 1].copy()

    fig, ax = plt.subplots(figsize=(13, 6))
    for _, r in b.iterrows():
        t = mdates.date2num(r["ts_utc"].to_pydatetime())
        color = "green" if r["close"] >= r["open"] else "red"
        ax.plot([t, t], [r["low"], r["high"]], color=color, linewidth=1)
        body_low = min(r["open"], r["close"])
        ax.add_patch(plt.Rectangle((t - 0.0008, body_low), 0.0016, abs(r["close"] - r["open"]) + 1e-8, color=color, alpha=0.7))

    geom = _to_dict(pat.get("geometry_params"))
    if "neckline_slope" in geom and "neckline_intercept" in geom:
        x = pd.Series(range(lo, hi + 1))
        y = geom["neckline_slope"] * x + geom["neckline_intercept"]
        ax.plot(b["ts_utc"], y, linestyle="--", label="neckline")
    if "upper_slope" in geom and "upper_intercept" in geom:
        x = pd.Series(range(lo, hi + 1))
        ax.plot(b["ts_utc"], geom["upper_slope"] * x + geom["upper_intercept"], linestyle="--", label="upper boundary")
    if "lower_slope" in geom and "lower_intercept" in geom:
        x = pd.Series(range(lo, hi + 1))
        ax.plot(b["ts_utc"], geom["lower_slope"] * x + geom["lower_intercept"], linestyle="--", label="lower boundary")

    ax.axvline(pat["t_confirm_utc"], color="blue", linestyle=":", label="confirmation")
    ax.set_title(f"{asset} {timeframe} {pat['pattern_type']} {pat['pattern_id']}")
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%Y-%m-%d\n%H:%M"))
    ax.legend(loc="best")
    fig.autofmt_xdate()

    if save_path:
        save_path.parent.mkdir(parents=True, exist_ok=True)
        fig.savefig(save_path, dpi=150, bbox_inches="tight")
        print(f"saved {save_path}")
    else:
        plt.show()


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description="Visual review for detected pattern instances")
    ap.add_argument("--run-root", required=True)
    ap.add_argument("--asset", required=True)
    ap.add_argument("--timeframe", required=True)
    ap.add_argument("--pattern-id")
    ap.add_argument("--pattern-type")
    ap.add_argument("--window-bars", type=int, default=80)
    ap.add_argument("--show-outcome", action="store_true")
    ap.add_argument("--save")
    args = ap.parse_args()

    plot_detection(
        Path(args.run_root),
        args.asset,
        args.timeframe,
        args.pattern_id,
        args.pattern_type,
        args.window_bars,
        args.show_outcome,
        Path(args.save) if args.save else None,
    )
