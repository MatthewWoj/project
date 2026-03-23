# Saved Outputs Catalog

All outputs are written under: `data/outputs/<run_name>/`

- `clean_1m/<ASSET>_1m.parquet` — cleaned canonical 1-minute bars.
- `bars/<ASSET>_<TF>.parquet` — reconstructed bars (3m/5m/15m/1h/4h/1d/1w).
- `turning_points/<ASSET>_<TF>_pivots.parquet` — turning points for the configured pivot method (`atr_zigzag` or `smoothed_extrema`).
- `patterns/<ASSET>_<TF>_patterns.parquet` — pattern instances with geometry/context/outcome payloads.
- `outcomes/<ASSET>_<TF>_outcomes.parquet` — per-pattern outcome labels per asset/timeframe.
- `outcomes/all_patterns_with_outcomes.parquet` — consolidated outcomes with nesting links.
- `significance/surrogate_stats.parquet` — surrogate simulation pattern statistics.
- `significance/existence_significance_perm.parquet` — permutation null significance.
- `significance/existence_significance_stationary.parquet` — stationary bootstrap null significance.
- `experiments/transfer_summary.parquet` — transfer analysis summary.
- `experiments/nesting_summary.parquet` — nesting summary across timeframes.
- `experiments/failure_by_session.parquet` — failure by session bucket.
- `experiments/failure_by_vol_regime.parquet` — failure by volatility regime.
- `experiments/macro_event_labels.parquet` — event-window label per `pattern_id` (if events CSV is configured).
- `experiments/data_quality.parquet` — missing/duplicate/stale diagnostics.
- `figures/failure_by_session.png` — reporting plot.
- `meta/config_used.yaml` and `meta/locked_parameters.parquet` — reproducibility metadata.

Interpretability note:
- Pattern files now carry richer debug payloads inside `geometry_params`, intended for manual chart review and dissertation figures (pivot labels, fitted boundaries, score components, confirmation semantics, detector variant).
