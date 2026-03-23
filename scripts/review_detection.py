from __future__ import annotations

import argparse
import json
from pathlib import Path

import matplotlib.dates as mdates
import matplotlib.pyplot as plt
import numpy as np
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


def _plot_pivots(ax, pivots_used):
    for pivot in pivots_used:
        ts = pd.Timestamp(pivot["ts_utc"])
        price = float(pivot["pivot_price"])
        marker = "^" if pivot.get("pivot_type") == "HIGH" else "v"
        color = "purple" if pivot.get("pivot_type") == "HIGH" else "orange"
        ax.scatter(ts, price, marker=marker, color=color, s=70, zorder=5)
        ax.annotate(
            pivot.get("label", ""),
            (ts, price),
            textcoords="offset points",
            xytext=(0, 8 if marker == "^" else -14),
            ha="center",
            color=color,
            fontsize=8,
        )


def _visible_index_series(bars: pd.DataFrame) -> np.ndarray:
    return bars.index.to_numpy(dtype=float)


def _bars_since_start(bars: pd.DataFrame, start_ts: pd.Timestamp, end_ts: pd.Timestamp | None = None) -> np.ndarray:
    values = np.full(len(bars), np.nan)
    active = bars["ts_utc"] >= start_ts
    if end_ts is not None:
        active &= bars["ts_utc"] <= end_ts
    count = 0
    for i, use in enumerate(active):
        if use:
            values[i] = count
            count += 1
    return values


def _line_values(line: dict, bars: pd.DataFrame, geom: dict) -> np.ndarray | None:
    if not line:
        return None
    indices = _visible_index_series(bars)
    if line.get("kind") == "horizontal":
        values = np.full(len(indices), float(line["value"]))
    else:
        index_mode = line.get("index_mode")
        if index_mode == "bars_since_start":
            start_ts = pd.Timestamp(line.get("start_ts", geom.get("t_start_utc", bars["ts_utc"].iloc[0])))
            end_ts = pd.Timestamp(line["end_ts"]) if line.get("end_ts") else None
            x = _bars_since_start(bars, start_ts, end_ts)
        else:
            indices = _visible_index_series(bars)
            start_idx = int(geom.get("candidate_window_bounds", {}).get("start_idx", int(indices[0]) if len(indices) else 0))
            if index_mode == "local_from_start":
                x = indices - start_idx
            else:
                x = indices
        values = float(line["slope"]) * x + float(line["intercept"])
    if line.get("kind") == "horizontal":
        values = values.astype(float)
    if line.get("coordinate_system") == "log_price":
        values = np.exp(values)
    return values


def _plot_line(ax, bars: pd.DataFrame, geom: dict, line: dict, label: str, style: str) -> bool:
    values = _line_values(line, bars, geom)
    if values is None:
        return False
    price_low = float(bars["low"].min())
    price_high = float(bars["high"].max())
    span = max(price_high - price_low, 1e-9)
    if np.nanmin(values) < price_low - 5 * span or np.nanmax(values) > price_high + 5 * span:
        return False
    ax.plot(bars["ts_utc"], values, style, label=label, linewidth=1.6)
    return True


def _add_annotation(ax, geom: dict, pat: pd.Series):
    score_components = geom.get("score_components", {})
    confirmation_reason = geom.get("confirmation_reason")
    status = geom.get("detection_status")
    lines = [
        f"{pat['pattern_type']} | {pat['detector_name']}",
        f"score={pat['score']:.4f}",
    ]
    if status:
        lines.append(f"status={status}")
    if confirmation_reason:
        lines.append(f"confirm={confirmation_reason}")
    for key in list(score_components)[:4]:
        lines.append(f"{key}={score_components[key]:.4f}" if isinstance(score_components[key], (int, float)) else f"{key}={score_components[key]}")
    ax.text(
        0.01,
        0.99,
        "\n".join(lines),
        transform=ax.transAxes,
        va="top",
        ha="left",
        fontsize=8,
        bbox={"facecolor": "white", "alpha": 0.8, "edgecolor": "gray"},
    )


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
    if len(b) >= 2:
        delta = np.median(np.diff(mdates.date2num(b["ts_utc"].dt.to_pydatetime())))
        candle_width = max(delta * 0.65, 0.0008)
    else:
        candle_width = 0.02
    for _, r in b.iterrows():
        t = mdates.date2num(r["ts_utc"].to_pydatetime())
        color = "green" if r["close"] >= r["open"] else "red"
        ax.plot([t, t], [r["low"], r["high"]], color=color, linewidth=1)
        body_low = min(r["open"], r["close"])
        body_height = max(abs(r["close"] - r["open"]), max((b["high"] - b["low"]).median() * 0.03, 1e-8))
        ax.add_patch(plt.Rectangle((t - candle_width / 2, body_low), candle_width, body_height, color=color, alpha=0.7))

    geom = _to_dict(pat.get("geometry_params"))
    fitted = geom.get("fitted_lines", {})
    if "neckline" in fitted:
        _plot_line(ax, b, geom, fitted["neckline"], "neckline", "--")
    elif "neckline_slope" in geom and "neckline_intercept" in geom:
        legacy = {
            "kind": "affine",
            "slope": geom["neckline_slope"],
            "intercept": geom["neckline_intercept"],
            "coordinate_system": "raw_price",
            "index_mode": "global",
        }
        _plot_line(ax, b, geom, legacy, "neckline", "--")
    if "upper" in fitted:
        _plot_line(ax, b, geom, fitted["upper"], "upper boundary", "--")
    elif "upper_slope" in geom and "upper_intercept" in geom:
        legacy = {
            "kind": "affine",
            "slope": geom["upper_slope"],
            "intercept": geom["upper_intercept"],
            "coordinate_system": "raw_price",
            "index_mode": "global",
        }
        _plot_line(ax, b, geom, legacy, "upper boundary", "--")
    if "lower" in fitted:
        _plot_line(ax, b, geom, fitted["lower"], "lower boundary", "--")
    elif "lower_slope" in geom and "lower_intercept" in geom:
        legacy = {
            "kind": "affine",
            "slope": geom["lower_slope"],
            "intercept": geom["lower_intercept"],
            "coordinate_system": "raw_price",
            "index_mode": "global",
        }
        _plot_line(ax, b, geom, legacy, "lower boundary", "--")
    if "center" in fitted:
        _plot_line(ax, b, geom, fitted["center"], "centerline", "-.")

    if geom.get("detection_status") != "STRUCTURAL_ONLY":
        if b["ts_utc"].min() <= pat["t_confirm_utc"] <= b["ts_utc"].max():
            ax.axvline(pat["t_confirm_utc"], color="blue", linestyle=":", label="confirmation")
    else:
        ax.axvspan(pat["t_start_utc"], pat["t_end_utc"], color="blue", alpha=0.08, label="structure window")

    _plot_pivots(ax, geom.get("pivots_used", []))
    _add_annotation(ax, geom, pat)
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
