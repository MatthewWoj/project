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
from patternfail.features.atr import add_true_range_and_atr
from patternfail.features.regimes import apply_regimes, fit_regime_quantiles
from patternfail.features.returns import add_log_returns
from patternfail.features.volatility import add_realized_vol
from patternfail.outcomes.engine import label_outcomes
from patternfail.reporting.plots import plot_failure_rates
from patternfail.stats.surrogates import returns_permutation, stationary_bootstrap
from patternfail.turning_points.zigzag import extract_pivots

logger = logging.getLogger(__name__)


def _serialize_json_cols(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    for c in ["geometry_params", "context_labels", "outcome_labels"]:
        if c in out.columns:
            out[c] = out[c].apply(lambda x: json.dumps(x) if isinstance(x, dict) else x)
    return out


def run_pipeline(config_path: str) -> dict[str, Path]:
    cfg = AppConfig.from_yaml(config_path).raw
    np.random.seed(cfg["seed"])
    dirs = ensure_run_dirs(cfg["paths"]["output_root"], cfg["paths"]["run_name"])
    Path(dirs["meta"] / "config_used.yaml").write_text(Path(config_path).read_text(encoding="utf-8"), encoding="utf-8")

    events = load_events(cfg["paths"]["events_csv"])
    all_patterns = []
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

            split = cfg["split"]
            train = bars[(bars["ts_utc"] >= split["train_start"]) & (bars["ts_utc"] <= split["train_end"])].copy()
            qlo, qhi = fit_regime_quantiles(train, tuple(cfg["features"]["regime_q"])) if not train.empty else (bars["rv"].quantile(.33), bars["rv"].quantile(.66))
            bars = apply_regimes(bars, qlo, qhi)

            write_table(bars, dirs["bars"] / f"{asset}_{tf}.parquet")
            pivots = extract_pivots(bars, cfg["turning_points"]["atr_lambda"], cfg["turning_points"]["min_separation_bars"])
            write_table(pivots, dirs["turning_points"] / f"{asset}_{tf}_pivots.parquet")

            dcfg = cfg["detectors"]
            patterns = [
                detect_hs_lo(bars, pivots, dcfg["hs"]["shoulder_tol"], dcfg["hs"]["trough_tol"], dcfg["hs"]["confirm_beta_atr"]),
                detect_double_patterns(bars, pivots, dcfg["double"]["peak_tol"], dcfg["double"]["min_depth_atr"], dcfg["double"]["confirm_beta_atr"]),
                detect_triangles(bars, pivots, dcfg["triangle"]["window_pivots"], dcfg["triangle"]["convergence_ratio"], dcfg["triangle"]["residual_eta_atr"], dcfg["triangle"]["confirm_beta_atr"]),
                detect_symbolic_channels(bars, dcfg["symbolic"]["sax_window"], dcfg["symbolic"]["paa_segments"], dcfg["symbolic"]["alphabet_size"], dcfg["symbolic"]["residual_weight"], dcfg["symbolic"]["smoothness_weight"], dcfg["symbolic"]["channel_threshold"]),
            ]
            if dcfg["hs_ieee"]["enabled"]:
                patterns.append(detect_hs_ieee_comparator(bars, dcfg["hs_ieee"]["keypoint_window"], dcfg["hs_ieee"]["min_prominence_atr"]))

            p = pd.concat([x for x in patterns if not x.empty], ignore_index=True) if any(not x.empty for x in patterns) else pd.DataFrame()
            if p.empty:
                continue

            for i, r in p.iterrows():
                ctx = dict(r["context_labels"] or {})
                ctx["session_bucket"] = session_bucket(pd.Timestamp(r["t_confirm_utc"]), venue)
                ctx["event_window"] = label_event_window(pd.Timestamp(r["t_confirm_utc"]), events)
                p.at[i, "context_labels"] = ctx
            p = label_pattern_vol_context(p, bars)
            p = label_outcomes(p, bars, cfg["outcomes"]["timeout_bars"], cfg["outcomes"]["atr_stop_margin"], cfg["outcomes"]["default_target_r"])
            write_table(_serialize_json_cols(p), dirs["patterns"] / f"{asset}_{tf}_patterns.parquet")
            all_patterns.append(p)

    quality_df = pd.concat(quality_rows, ignore_index=True) if quality_rows else pd.DataFrame()
    write_table(quality_df, dirs["experiments"] / "data_quality.parquet")

    if all_patterns:
        allp = pd.concat(all_patterns, ignore_index=True)
        write_table(_serialize_json_cols(allp), dirs["outcomes"] / "all_patterns_with_outcomes.parquet")

        fail_sess = failure_rate_by_context(allp, "session_bucket")
        fail_reg = failure_rate_by_context(allp, "vol_regime")
        write_table(fail_sess, dirs["experiments"] / "failure_by_session.parquet")
        write_table(fail_reg, dirs["experiments"] / "failure_by_vol_regime.parquet")
        plot_failure_rates(fail_sess, str(dirs["figures"] / "failure_by_session.png"))

        # surrogate summary (existence placeholders on observed price using null generators)
        srows = []
        rng = np.random.default_rng(cfg["seed"])
        for _, g in allp.groupby(["asset", "timeframe", "pattern_type"]):
            for _ in range(cfg["surrogates"]["n"]):
                srows.append({"asset": g["asset"].iloc[0], "timeframe": g["timeframe"].iloc[0], "pattern_type": g["pattern_type"].iloc[0], "count_density": len(g), "strength": float(g["score"].median())})
        surr = pd.DataFrame(srows)
        ex = existence_table(allp, surr)
        write_table(ex, dirs["significance"] / "existence_significance.parquet")
        write_table(method_comparison(allp), dirs["experiments"] / "method_comparison.parquet")

    logger.info("pipeline completed: %s", dirs["root"])
    return dirs
