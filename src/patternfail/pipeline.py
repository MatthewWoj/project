from __future__ import annotations

import json
import logging
from pathlib import Path

import numpy as np
import pandas as pd

from patternfail.common.config import AppConfig
from patternfail.common.io import write_table
from patternfail.common.paths import ensure_run_dirs
from patternfail.context.events import label_event_window, load_events
from patternfail.context.sessions import session_bucket
from patternfail.context.volatility_context import label_pattern_vol_context
from patternfail.data.bars import aggregate_bars
from patternfail.data.ingest import ingest_asset_csv
from patternfail.data.quality import missing_bar_report, stale_quote_flags
from patternfail.detectors.channels import detect_symbolic_channels
from patternfail.detectors.double_patterns import detect_double_patterns
from patternfail.detectors.head_shoulders import detect_hs_ieee_comparator, detect_hs_lo
from patternfail.detectors.triangles import detect_triangles
from patternfail.experiments.existence import existence_table
from patternfail.experiments.failure_context import failure_rate_by_context
from patternfail.experiments.method_compare import method_comparison
from patternfail.experiments.nesting import apply_nesting, nesting_summary
from patternfail.experiments.transfer import transfer_summary
from patternfail.features.atr import add_true_range_and_atr
from patternfail.features.regimes import apply_regimes, fit_regime_quantiles
from patternfail.features.returns import add_log_returns
from patternfail.features.volatility import add_realized_vol
from patternfail.outcomes.engine import label_outcomes
from patternfail.reporting.plots import plot_failure_rates
from patternfail.reporting.tables import parameter_table
from patternfail.stats.surrogates import returns_permutation, stationary_bootstrap
from patternfail.turning_points.zigzag import extract_pivots

logger = logging.getLogger(__name__)


def _serialize_json_cols(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    for c in ["geometry_params", "context_labels", "outcome_labels"]:
        if c in out.columns:
            out[c] = out[c].apply(lambda x: json.dumps(x) if isinstance(x, dict) else x)
    return out


def _read_table(path: Path) -> pd.DataFrame:
    if path.suffix == ".parquet" and path.exists():
        return pd.read_parquet(path)
    alt = path.with_suffix(".csv")
    if alt.exists():
        return pd.read_csv(alt)
    raise FileNotFoundError(path)


def _detect_patterns(bars: pd.DataFrame, cfg: dict, pivots: pd.DataFrame) -> pd.DataFrame:
    dcfg = cfg["detectors"]
    patterns = [
        detect_hs_lo(bars, pivots, dcfg["hs"]["shoulder_tol"], dcfg["hs"]["trough_tol"], dcfg["hs"]["confirm_beta_atr"]),
        detect_double_patterns(bars, pivots, dcfg["double"]["peak_tol"], dcfg["double"]["min_depth_atr"], dcfg["double"]["confirm_beta_atr"]),
        detect_triangles(
            bars,
            pivots,
            dcfg["triangle"]["window_pivots"],
            dcfg["triangle"]["convergence_ratio"],
            dcfg["triangle"]["residual_eta_atr"],
            dcfg["triangle"]["confirm_beta_atr"],
        ),
        detect_symbolic_channels(
            bars,
            dcfg["symbolic"]["sax_window"],
            dcfg["symbolic"]["paa_segments"],
            dcfg["symbolic"]["alphabet_size"],
            dcfg["symbolic"]["residual_weight"],
            dcfg["symbolic"]["smoothness_weight"],
            dcfg["symbolic"]["channel_threshold"],
        ),
    ]
    if dcfg["hs_ieee"]["enabled"]:
        patterns.append(detect_hs_ieee_comparator(bars, dcfg["hs_ieee"]["keypoint_window"], dcfg["hs_ieee"]["min_prominence_atr"]))
    return pd.concat([x for x in patterns if not x.empty], ignore_index=True) if any(not x.empty for x in patterns) else pd.DataFrame()


def _surrogate_bars_from_test(test_bars: pd.DataFrame, surrogate_close: pd.Series) -> pd.DataFrame:
    s = test_bars.copy()
    s["close"] = surrogate_close.values
    s["open"] = s["close"].shift(1).fillna(s["close"])
    s["high"] = s[["open", "close"]].max(axis=1)
    s["low"] = s[["open", "close"]].min(axis=1)
    s["volume"] = 0.0
    return s


def _run_data_layer(cfg: dict, dirs: dict[str, Path]) -> None:
    quality_rows = []
    for asset in cfg["assets"]:
        venue = cfg["venues"][asset]
        df1m, rep = ingest_asset_csv(asset, venue, cfg["input_csv"][asset], cfg["csv"])
        df1m = stale_quote_flags(df1m, cfg["quality"]["stale_close_run"], cfg["quality"]["stale_zero_range_run"])
        q = missing_bar_report(df1m, venue)
        q["asset"] = asset
        q["duplicate_count"] = rep.duplicate_count
        q["invalid_row_count"] = rep.invalid_ohlc_count
        quality_rows.append(q)
        write_table(df1m, dirs["clean"] / f"{asset}_1m.parquet")

        for tf in cfg["timeframes"]:
            bars = aggregate_bars(df1m, tf, venue)
            bars = add_log_returns(bars)
            bars = add_true_range_and_atr(bars, cfg["features"]["atr_period"])
            bars = add_realized_vol(bars, cfg["features"]["rv_window"])
            write_table(bars, dirs["bars"] / f"{asset}_{tf}.parquet")

            pivots = extract_pivots(bars, cfg["turning_points"]["atr_lambda"], cfg["turning_points"]["min_separation_bars"])
            write_table(pivots, dirs["turning_points"] / f"{asset}_{tf}_pivots.parquet")

    quality_df = pd.concat(quality_rows, ignore_index=True) if quality_rows else pd.DataFrame()
    write_table(quality_df, dirs["experiments"] / "data_quality.parquet")


def _run_detection_layer(cfg: dict, dirs: dict[str, Path]) -> tuple[pd.DataFrame, pd.DataFrame]:
    split = cfg["split"]
    events = load_events(cfg["paths"]["events_csv"])
    all_patterns = []
    macro_rows = []
    surrogate_rows: list[dict] = []
    rng = np.random.default_rng(cfg["seed"])

    for asset in cfg["assets"]:
        venue = cfg["venues"][asset]
        for tf in cfg["timeframes"]:
            bars = _read_table(dirs["bars"] / f"{asset}_{tf}.parquet")
            bars["ts_utc"] = pd.to_datetime(bars["ts_utc"], utc=True)
            pivots = _read_table(dirs["turning_points"] / f"{asset}_{tf}_pivots.parquet")

            train = bars[(bars["ts_utc"] >= split["train_start"]) & (bars["ts_utc"] <= split["train_end"])].copy()
            test = bars[(bars["ts_utc"] >= split["test_start"]) & (bars["ts_utc"] <= split["test_end"])].copy().reset_index(drop=True)
            if test.empty:
                continue

            qlo, qhi = fit_regime_quantiles(train, tuple(cfg["features"]["regime_q"])) if not train.empty else (bars["rv"].quantile(0.33), bars["rv"].quantile(0.66))
            test = apply_regimes(test, qlo, qhi)
            p = _detect_patterns(test, cfg, pivots)
            if p.empty:
                continue

            for i, row in p.iterrows():
                ctx = dict(row["context_labels"] or {})
                event_label = label_event_window(pd.Timestamp(row["t_confirm_utc"]), events)
                ctx["session_bucket"] = session_bucket(pd.Timestamp(row["t_confirm_utc"]), venue)
                ctx["event_window"] = event_label
                p.at[i, "context_labels"] = ctx
                macro_rows.append({"pattern_id": row["pattern_id"], "asset": row["asset"], "timeframe": row["timeframe"], "event_window": event_label})

            p = label_pattern_vol_context(p, test)
            p = label_outcomes(p, test, cfg["outcomes"]["timeout_bars"], cfg["outcomes"]["atr_stop_margin"], cfg["outcomes"]["default_target_r"])
            write_table(_serialize_json_cols(p), dirs["patterns"] / f"{asset}_{tf}_patterns.parquet")
            write_table(_serialize_json_cols(p), dirs["outcomes"] / f"{asset}_{tf}_outcomes.parquet")
            all_patterns.append(p)

            for sidx in range(cfg["surrogates"]["n"]):
                for null_name in ("perm", "stationary"):
                    s_close = (
                        returns_permutation(test["close"], rng)
                        if null_name == "perm"
                        else stationary_bootstrap(test["close"], cfg["surrogates"]["stationary_bootstrap_block"], rng)
                    )
                    s_bars = _surrogate_bars_from_test(test, s_close)
                    s_bars = add_log_returns(s_bars)
                    s_bars = add_true_range_and_atr(s_bars, cfg["features"]["atr_period"])
                    s_bars = add_realized_vol(s_bars, cfg["features"]["rv_window"])
                    s_bars = apply_regimes(s_bars, qlo, qhi)
                    s_pivots = extract_pivots(s_bars, cfg["turning_points"]["atr_lambda"], cfg["turning_points"]["min_separation_bars"])
                    s_patterns = _detect_patterns(s_bars, cfg, s_pivots)
                    if s_patterns.empty:
                        continue
                    for k, g in s_patterns.groupby(["asset", "timeframe", "pattern_type"]):
                        surrogate_rows.append(
                            {
                                "asset": k[0],
                                "timeframe": k[1],
                                "pattern_type": k[2],
                                "null_model": null_name,
                                "sim_id": sidx,
                                "count_density": len(g),
                                "strength": float(g["score"].median()),
                            }
                        )

    macro_df = pd.DataFrame(macro_rows)
    write_table(macro_df, dirs["experiments"] / "macro_event_labels.parquet")
    surr_df = pd.DataFrame(surrogate_rows)
    if not surr_df.empty:
        write_table(surr_df, dirs["significance"] / "surrogate_stats.parquet")

    return pd.concat(all_patterns, ignore_index=True) if all_patterns else pd.DataFrame(), surr_df


def _run_experiments_layer(cfg: dict, dirs: dict[str, Path], all_patterns: pd.DataFrame | None = None, surrogate_stats: pd.DataFrame | None = None) -> None:
    if all_patterns is None:
        frames = []
        for asset in cfg["assets"]:
            for tf in cfg["timeframes"]:
                pfile = dirs["patterns"] / f"{asset}_{tf}_patterns.parquet"
                try:
                    p = _read_table(pfile)
                except FileNotFoundError:
                    continue
                if p.empty:
                    continue
                for col in ["geometry_params", "context_labels", "outcome_labels"]:
                    if col in p.columns and p[col].dtype == object:
                        p[col] = p[col].apply(lambda x: json.loads(x) if isinstance(x, str) and x.startswith("{") else x)
                frames.append(p)
        all_patterns = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()

    if all_patterns.empty:
        logger.warning("no pattern files available for experiments layer")
        return

    rank = {"3min": 1, "5min": 2, "15min": 3, "1h": 4, "4h": 5, "1d": 6, "1w": 7}
    nested_parts = []
    for asset, g in all_patterns.groupby("asset"):
        for tf_low in sorted(g["timeframe"].unique(), key=lambda x: rank.get(x, 999)):
            lows = g[g["timeframe"] == tf_low]
            uppers = g[g["timeframe"].map(lambda x: rank.get(x, 0) > rank.get(tf_low, 0))]
            nested_parts.append(apply_nesting(lows, uppers) if not uppers.empty else lows)
    allp = pd.concat(nested_parts, ignore_index=True) if nested_parts else all_patterns

    write_table(_serialize_json_cols(allp), dirs["outcomes"] / "all_patterns_with_outcomes.parquet")

    write_table(failure_rate_by_context(allp, "session_bucket"), dirs["experiments"] / "failure_by_session.parquet")
    fail_reg = failure_rate_by_context(allp, "vol_regime")
    write_table(fail_reg, dirs["experiments"] / "failure_by_vol_regime.parquet")
    write_table(transfer_summary(allp, cfg.get("experiments", {}).get("base_timeframe", "15min")), dirs["experiments"] / "transfer_summary.parquet")
    write_table(nesting_summary(allp), dirs["experiments"] / "nesting_summary.parquet")
    write_table(method_comparison(allp), dirs["experiments"] / "method_comparison.parquet")
    plot_failure_rates(_read_table(dirs["experiments"] / "failure_by_session.parquet"), str(dirs["figures"] / "failure_by_session.png"))

    if surrogate_stats is None:
        try:
            surrogate_stats = _read_table(dirs["significance"] / "surrogate_stats.parquet")
        except FileNotFoundError:
            surrogate_stats = pd.DataFrame()
    if not surrogate_stats.empty:
        for null_name, g in surrogate_stats.groupby("null_model"):
            write_table(existence_table(allp, g), dirs["significance"] / f"existence_significance_{null_name}.parquet")


def run_pipeline(config_path: str, stage: str = "full") -> dict[str, Path]:
    cfg = AppConfig.from_yaml(config_path).raw
    np.random.seed(cfg["seed"])
    dirs = ensure_run_dirs(cfg["paths"]["output_root"], cfg["paths"]["run_name"])
    Path(dirs["meta"] / "config_used.yaml").write_text(Path(config_path).read_text(encoding="utf-8"), encoding="utf-8")
    write_table(parameter_table(cfg), dirs["meta"] / "locked_parameters.parquet")

    if stage in ("full", "data"):
        _run_data_layer(cfg, dirs)
    if stage in ("full", "detect"):
        patterns, surr = _run_detection_layer(cfg, dirs)
    else:
        patterns, surr = None, None
    if stage in ("full", "experiments"):
        _run_experiments_layer(cfg, dirs, patterns, surr)

    logger.info("pipeline completed (%s): %s", stage, dirs["root"])
    return dirs
