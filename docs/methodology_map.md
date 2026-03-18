# Methodology-to-Implementation Map

| Module/Experiment | Dissertation Aim | Status |
|---|---|---|
| `turning_points/zigzag.py` (ATR ZigZag pivots; Lo-style structure extraction) | Robust turning-point based structural decomposition | literature-grounded |
| `detectors/head_shoulders.py::detect_hs_lo` | Formalized H&S/IHS detector from turning points | literature-grounded |
| `detectors/head_shoulders.py::detect_hs_ieee_comparator` | IEEE-style comparator baseline for H&S | literature-grounded |
| `detectors/double_patterns.py` (DT/DB) | Double top/bottom detection from pivots | literature-grounded |
| `detectors/triangles.py` | Triangle pattern formalization and confirmation | literature-grounded |
| `detectors/symbolic_sax.py` + `detectors/channels.py` | Symbolic channels inspired by SAX/CPC-SAX logic | practical implementation choice |
| `stats/surrogates.py` permutation null | Null-model existence checks | literature-grounded |
| `stats/surrogates.py` stationary bootstrap | Dependence-preserving null model | literature-grounded |
| `stats/multiple_testing.py` BH-FDR | Multiple-testing control | literature-grounded |
| `experiments/transfer.py` | Cross-asset/timeframe transferability | practical implementation choice |
| `experiments/nesting.py` | Pattern nesting interactions across scales | practical implementation choice |
| `context/events.py` + `experiments/macro_event_labels.parquet` | Macro-event sensitivity | pilot |
| Additional patterns (wedges/flags), richer structural priors, and more event taxonomies | Extended dissertation roadmap | future work |

Notes:
- Detector thresholds, ATR multipliers, timeout mechanics, and session buckets are fixed by config for reproducibility (practical implementation choice).
- The repository currently focuses on stable core patterns and significance workflows rather than broad pattern expansion.
