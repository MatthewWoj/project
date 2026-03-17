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
