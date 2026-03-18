# Data Dictionary (Major Tables)

## clean_1m
- `asset`, `venue_type`, `ts_utc`, `open`, `high`, `low`, `close`, `volume`
- quality flags may be added (`stale_close_flag`, `stale_zero_range_flag`).

## bars
- All OHLCV fields + `n_underlying`, `completeness_flag`, `gap_flag`, `timeframe`, `asset`, `venue_type`, plus feature columns (`log_ret`, `tr`, `atr`, `rv`, `vol_regime`).

## turning_points
- `asset`, `timeframe`, `pivot_index`, `ts_utc`, `pivot_type`, `pivot_price`.

## patterns / outcomes
- `pattern_id`, `asset`, `venue_type`, `timeframe`, `pattern_type`, `direction`
- `t_start_utc`, `t_end_utc`, `t_confirm_utc`, `score`
- `detector_family`, `detector_name`
- `geometry_params` (JSON), `context_labels` (JSON), `outcome_labels` (JSON)
- `nested_in_pattern_id` (nullable)

## significance/surrogate_stats
- `asset`, `timeframe`, `pattern_type`, `null_model`, `sim_id`, `count_density`, `strength`.

## transfer_summary / nesting_summary / failure_by_* / method_comparison
- experiment-specific aggregate metrics indexed by asset/timeframe/pattern/context.

## macro_event_labels
- `pattern_id`, `asset`, `timeframe`, `event_window`.
