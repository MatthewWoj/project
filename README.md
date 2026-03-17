# When Do Chart Patterns Fail? — Research Pipeline

Production-quality Python repository for dissertation-grade chart-pattern research from raw 1m OHLCV to detection, outcomes, surrogate significance, and context experiments.

## Setup

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
pip install -e .
```

## Run

```bash
patternfail --config configs/default.yaml
# or
python examples/run_pipeline.py
```

## Repository structure

- `configs/default.yaml` — single run config (assets, paths, thresholds, split, seed).
- `src/patternfail/data` — ingestion, market-hours masks, quality diagnostics, bar aggregation.
- `src/patternfail/features` — returns/ATR/volatility/regimes.
- `src/patternfail/turning_points` — causal ATR-ZigZag pivots.
- `src/patternfail/detectors` — Lo-style H&S, IEEE comparator, DT/DB, triangles, symbolic channels.
- `src/patternfail/outcomes` — entry/stop/target/timeout/MFE-MAE labeling.
- `src/patternfail/stats` — surrogates, Monte Carlo significance, BH-FDR.
- `src/patternfail/context` — session/volatility/event context labels.
- `src/patternfail/experiments` — existence, failure context, transfer, nesting, method comparison.
- `src/patternfail/reporting` — tables + matplotlib plots.
- `src/patternfail/pipeline.py` — end-to-end orchestrator.
- `src/patternfail/cli.py` — CLI entrypoint.

## Methodology notes

- **Literature-grounded modules**: computational pattern formalization (Lo-style), SAX symbolic representation, surrogate testing, stationary bootstrap, BH-FDR.
- **Practical implementation choices**: exact detector thresholds, ATR multipliers, session buckets, and timeout rules (all locked in config).
- **Pilot-level**: macro-event window labeling via optional events CSV.
- **Future work**: wedges and bull/bear flags are intentionally out-of-scope for the stable core.

## Outputs

The pipeline saves deterministic run artifacts under `data/outputs/<run_name>/`:

- cleaned 1m tables
- reconstructed bars by timeframe
- turning points
- pattern detections and outcomes (with JSON-serializable geometry/context/outcome fields)
- data-quality and experiment tables
- surrogate stats and existence significance tables
- basic failure plots

## Visual validation recommendation

For manual post-hoc pattern validation, start with **mplfinance** for static OHLC overlays and switch to **plotly** for interactive inspection when needed.

## Notes

- UTC is canonical internal time.
- Train/test split is config-locked (default 2022–2024 train, 2025 test).
- No fabricated results are included; this repo implements the pipeline only.
# Pattern Failures Pipeline

Implementation-ready research scaffold for detecting chart patterns, labeling outcomes, and running surrogate-based significance tests.

## Modules

- `config`: locked thresholds and train/test period.
- `data_ingest`: canonical 1-minute schema and quality diagnostics.
- `market_calendars`: venue-aware trading masks.
- `bar_builder`: deterministic timeframe aggregation.
- `features`: ATR, returns, realized-volatility regimes.
- `turning_points`: ATR-scaled ZigZag pivots.
- `detectors_geometric`: HS/IHS, DT/DB, triangles.
- `symbolic_transform`: SAX + CPC-inspired symbol-diff features.
- `detectors_symbolic`: channel baseline detector.
- `outcomes`: post-confirmation success/failure mechanics.
- `surrogates`: permutation and stationary-bootstrap generators.
- `significance`: Monte Carlo p-values and BH-FDR.
- `experiments`/`reporting`: summary and publication tables.

## Quick start

```bash
pip install -e .
pytest -q
```
