# Methodology-to-Implementation Map

| Module/Experiment | Dissertation Aim | Status |
|---|---|---|
| `turning_points/zigzag.py` (ATR ZigZag pivots) | Volatility-scaled turning-point decomposition comparator | practical implementation choice |
| `turning_points/extrema.py` (smoothed local extrema pivots) | Comparator closer to Lo-style extrema extraction sensitivity | literature-grounded approximation |
| `detectors/head_shoulders.py::detect_hs_lo` | Formalized H&S/IHS detector from turning points | literature-grounded |
| `detectors/head_shoulders.py::detect_hs_ieee_comparator` | Key-point / sliding-window H&S comparator for method comparison | literature-grounded approximation |
| `detectors/double_patterns.py` (DT/DB) | Double top/bottom detection from pivots | literature-grounded |
| `detectors/triangles.py` | Triangle pattern formalization and confirmation | literature-grounded |
| `detectors/symbolic_sax.py` + `detectors/channels.py` | CPC-SAX-inspired symbolic channel baseline (ASCII-difference + regression structure) | literature-grounded approximation |
| `stats/surrogates.py` permutation null | Null-model existence checks | literature-grounded |
| `stats/surrogates.py` stationary bootstrap | Dependence-preserving null model | literature-grounded |
| `stats/multiple_testing.py` BH-FDR | Multiple-testing control | literature-grounded |
| `experiments/transfer.py` | Cross-asset/timeframe transferability | practical implementation choice |
| `experiments/nesting.py` | Pattern nesting interactions across scales | practical implementation choice |
| `context/events.py` + `experiments/macro_event_labels.parquet` | Macro-event sensitivity with explicit PRE / DURING / POST labels | pilot |
| Additional patterns (wedges/flags), richer structural priors, and more event taxonomies | Extended dissertation roadmap | future work |

Notes:
- Detector thresholds, ATR multipliers, timeout mechanics, sanity filters, and session buckets are fixed by config for reproducibility (practical implementation choice).
- `detect_hs_lo` remains the clearest literature-facing baseline for dissertation figures; the IEEE comparator is intentionally a transparent, research-prototype approximation rather than a claim of perfect paper reproduction.
- Channel detections are treated as **structural-only windows** unless an explicit confirmation rule is added later; this avoids plotting visually misleading fake confirmations.
- Debug/interpretability fields (`pivots_used`, `fitted_lines`, `score_components`, `confirmation_reason`, `detection_status`) are implementation choices added to improve manual validation and dissertation write-up quality.
