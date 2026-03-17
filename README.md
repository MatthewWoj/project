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

## Notes

- UTC is canonical internal time.
- Train/test split is config-locked (default 2022–2024 train, 2025 test).
- Outputs are written under `data/outputs/<run_name>/` with intermediate artifacts.
- No fabricated results are included; this repo implements the pipeline only.
