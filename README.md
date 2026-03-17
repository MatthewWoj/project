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
