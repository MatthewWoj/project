# First Run Testing Guide (Beginner-Friendly)

This guide is for validating the repo and then running it on your real files, including:

- `BTCUSDT_1m_2022_2025.csv`
- `NVDA_1min_2022_2025.csv`

## 1) Clone and install

```bash
git clone <your-repo-url>
cd project
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
pip install -e .
```

## 2) Sanity-check the repository

Run tests:

```bash
PYTHONPATH=src pytest -q
```

If this passes, your environment is good.

## 3) Prepare config for your CSVs

Start from:

```bash
cp configs/user_btc_nvda_example.yaml configs/my_run.yaml
```

Edit only:

- `input_csv.BTCUSDT`
- `input_csv.NVDA`
- `paths.run_name` (optional custom output folder name)
- On Windows, use forward slashes in YAML paths (for example `C:/Users/Mateo/data/BTCUSDT.csv`) or wrap backslash paths in single quotes.

### Why this config works for your exact files

- BTC file uses `open_time_utc` as timestamp and tab-separated columns.
- NVDA file uses `datetime_utc` with day-first date format (`03/01/2022` = 3 Jan 2022).

## 4) Run data layer first (recommended)

```bash
patternfail --config configs/my_run.yaml --stage data
```

Then verify these files exist:

- `data/outputs/<run_name>/clean_1m/BTCUSDT_1m.parquet` (or `.csv` fallback)
- `data/outputs/<run_name>/clean_1m/NVDA_1m.parquet` (or `.csv` fallback)
- `data/outputs/<run_name>/bars/BTCUSDT_3min.parquet` ... `BTCUSDT_1w.parquet`
- `data/outputs/<run_name>/bars/NVDA_3min.parquet` ... `NVDA_1w.parquet`

## 5) Run detection

```bash
patternfail --config configs/my_run.yaml --stage detect
```

Check outputs:

- `data/outputs/<run_name>/patterns/*_patterns.parquet`
- `data/outputs/<run_name>/outcomes/*_outcomes.parquet`
- `data/outputs/<run_name>/significance/surrogate_stats.parquet`

### Optional: quick progress-friendly detect run

For initial sanity checks, you can run fewer surrogates and see frequent progress logs:

```bash
patternfail --config configs/my_run.yaml --stage detect --surrogates-n 5 --max-workers 4 --progress-every 2
```

### Recommended setup for the bundled BTC + NVDA example

The starter config `configs/user_btc_nvda_example.yaml` now already defaults to a quicker first pass:

- `surrogates.n: 6`
- `runtime.max_workers: 8`
- `runtime.progress_every: 2`
- pattern de-duplication enabled

That means you can:

```bash
cp configs/user_btc_nvda_example.yaml configs/my_run.yaml
# edit input_csv.BTCUSDT and input_csv.NVDA
patternfail --config configs/my_run.yaml --stage data
patternfail --config configs/my_run.yaml --stage detect
```

If your machine has more CPU cores, a heavier but still practical command is:

```bash
patternfail --config configs/my_run.yaml --stage detect --surrogates-n 8 --max-workers 12 --progress-every 2
```

## 6) Run experiments/reporting

```bash
patternfail --config configs/my_run.yaml --stage experiments
```

Check outputs:

- `data/outputs/<run_name>/experiments/transfer_summary.parquet`
- `data/outputs/<run_name>/experiments/nesting_summary.parquet`
- `data/outputs/<run_name>/significance/existence_significance_perm.parquet`
- `data/outputs/<run_name>/significance/existence_significance_stationary.parquet`

## 7) Visual inspection of detections

```bash
python scripts/review_detection.py \
  --run-root data/outputs/<run_name> \
  --asset BTCUSDT \
  --timeframe 15min \
  --pattern-type HS_TOP \
  --window-bars 120 \
  --show-outcome \
  --save data/outputs/<run_name>/figures/review_btc_15min.png
```

You can also select a specific `--pattern-id`.

## Troubleshooting

- If files are comma-separated (not tab-separated), set `csv.asset_overrides.BTCUSDT.separator: ","`.
- If NVDA dates are interpreted incorrectly, keep `day_first: true` for NVDA.
- If parquet dependencies are unavailable, outputs will be written as CSV automatically.

## Interpretability / manual validation tips

- The review script reads either parquet or CSV outputs automatically and now uses richer debug payloads stored in `geometry_params`.
- Geometric detections should show pivot markers (`P1`, `P2`, ...) and fitted lines such as necklines or triangle boundaries.
- Channel detections are treated as structural windows by default, so the review artifact highlights the detected structure instead of drawing a visually misleading fake confirmation line.
- Fitted lines are now plotted in the correct coordinate space (for example channel lines are converted back from log-price space before being overlaid on raw-price candles).
