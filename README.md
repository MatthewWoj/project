# Pattern Failures Research Pipeline

A dissertation-oriented pipeline that ingests raw 1-minute OHLCV CSV files, reconstructs multi-timeframe bars, detects chart patterns, labels outcomes, and runs surrogate-based significance experiments.

## Quick Start

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
pip install -e .
```

### Windows notes

On Windows PowerShell, use:

```powershell
py -3.11 -m venv .venv
.\.venv\Scripts\Activate.ps1
py -m pip install -r requirements.txt
py -m pip install -e .
```

If `py -m pip install -e .` fails, confirm Python is 3.11+ (`py -V`) because this project requires Python >= 3.11.

### 1) Copy and edit config

Start from:

- `configs/default.yaml` (baseline)
- `configs/multi_asset_example.yaml` (real multi-asset CSV example)

Set your real file paths in `input_csv`.

### 2) Run commands

- Full pipeline (recommended first run):

```bash
patternfail --config configs/multi_asset_example.yaml --stage full
```

- Data layer only (ingest + reconstructed bars + pivots):

```bash
patternfail --config configs/multi_asset_example.yaml --stage data
```

- Detection only (reads saved bars/pivots and writes patterns/outcomes/significance stats):

```bash
patternfail --config configs/multi_asset_example.yaml --stage detect
```

- Experiments/reporting only (reads saved pattern outputs):

```bash
patternfail --config configs/multi_asset_example.yaml --stage experiments
```

### Example: single asset

Create a config with:

```yaml
assets: [AAPL]
input_csv:
  AAPL: /ABSOLUTE/PATH/TO/AAPL_1m.csv
```

Then run:

```bash
patternfail --config configs/aapl_only.yaml --stage full
```

### Example: multi asset

Use `configs/multi_asset_example.yaml` with assets:

- AAPL, NVDA, SPY, QQQ
- EURUSD, GBPUSD, XAUUSD, BTCUSDT

## New-user walkthrough for your BTC + NVDA files

If you are starting from scratch, use `docs/first_run_testing_guide.md`.

It includes exact steps for:

- `BTCUSDT_1m_2022_2025.csv`
- `NVDA_1min_2022_2025.csv`

and uses `configs/user_btc_nvda_example.yaml` as the starter config.

## CSV Schema and timezone behavior

See `docs/csv_schema.md`.

Highlights:

- Flexible column mapping via `csv.cols` and per-asset `csv.asset_overrides`.
- Naive timestamps are localized using `assume_tz` then converted to UTC.
- Internal canonical timestamp is always `ts_utc` (timezone-aware UTC).

## Output locations

All artifacts are written under:

```text
data/outputs/<run_name>/
```

See:

- `docs/output_catalog.md`
- `docs/data_dictionary.md`

## Visual validation (manual pattern review)

Use the review utility to browse by asset/timeframe/pattern type/pattern id and render candle windows with overlays.

```bash
python scripts/review_detection.py \
  --run-root data/outputs/real_csv_research \
  --asset AAPL \
  --timeframe 15min \
  --pattern-type HS_TOP \
  --window-bars 100 \
  --show-outcome \
  --save data/outputs/real_csv_research/figures/review_AAPL_15min.png
```

## Methodology mapping

See `docs/methodology_map.md` for explicit labels:

- literature-grounded
- practical implementation choice
- pilot
- future work

## Tests

```bash

```bash
patternfail --config configs/multi_asset_example.yaml --stage data
```

- Detection only (reads saved bars/pivots and writes patterns/outcomes/significance stats):

```bash
patternfail --config configs/multi_asset_example.yaml --stage detect
```

- Experiments/reporting only (reads saved pattern outputs):

```bash
patternfail --config configs/multi_asset_example.yaml --stage experiments
```

### Example: single asset

Create a config with:

```yaml
assets: [AAPL]
input_csv:
  AAPL: /ABSOLUTE/PATH/TO/AAPL_1m.csv
```

Then run:

```bash
patternfail --config configs/aapl_only.yaml --stage full
```

### Example: multi asset

Use `configs/multi_asset_example.yaml` with assets:

- AAPL, NVDA, SPY, QQQ
- EURUSD, GBPUSD, XAUUSD, BTCUSDT

## New-user walkthrough for your BTC + NVDA files

If you are starting from scratch, use `docs/first_run_testing_guide.md`.

It includes exact steps for:

- `BTCUSDT_1m_2022_2025.csv`
- `NVDA_1min_2022_2025.csv`

and uses `configs/user_btc_nvda_example.yaml` as the starter config.

## CSV Schema and timezone behavior

See `docs/csv_schema.md`.

Highlights:

- Flexible column mapping via `csv.cols` and per-asset `csv.asset_overrides`.
- Naive timestamps are localized using `assume_tz` then converted to UTC.
- Internal canonical timestamp is always `ts_utc` (timezone-aware UTC).

## Output locations

All artifacts are written under:

```text
data/outputs/<run_name>/
```

See:

- `docs/output_catalog.md`
- `docs/data_dictionary.md`


```bash
patternfail --config configs/multi_asset_example.yaml --stage full
```

- Data layer only (ingest + reconstructed bars + pivots):

```bash
patternfail --config configs/multi_asset_example.yaml --stage data
```

- Detection only (reads saved bars/pivots and writes patterns/outcomes/significance stats):

```bash
patternfail --config configs/multi_asset_example.yaml --stage detect
```

- Experiments/reporting only (reads saved pattern outputs):

```bash
patternfail --config configs/multi_asset_example.yaml --stage experiments
```

### Example: single asset

Create a config with:

```yaml
assets: [AAPL]
input_csv:
  AAPL: /ABSOLUTE/PATH/TO/AAPL_1m.csv
```

Then run:

```bash
patternfail --config configs/aapl_only.yaml --stage full
```

### Example: multi asset

Use `configs/multi_asset_example.yaml` with assets:

- AAPL, NVDA, SPY, QQQ
- EURUSD, GBPUSD, XAUUSD, BTCUSDT

## CSV Schema and timezone behavior

See `docs/csv_schema.md`.

Highlights:

- Flexible column mapping via `csv.cols` and per-asset `csv.asset_overrides`.
- Naive timestamps are localized using `assume_tz` then converted to UTC.
- Internal canonical timestamp is always `ts_utc` (timezone-aware UTC).

## Output locations

All artifacts are written under:

```text
data/outputs/<run_name>/
```

See:

- `docs/output_catalog.md`
- `docs/data_dictionary.md`

## Visual validation (manual pattern review)

Use the review utility to browse by asset/timeframe/pattern type/pattern id and render candle windows with overlays.

```bash
python scripts/review_detection.py \
  --run-root data/outputs/real_csv_research \
  --asset AAPL \
  --timeframe 15min \
  --pattern-type HS_TOP \
  --window-bars 100 \
  --show-outcome \
  --save data/outputs/real_csv_research/figures/review_AAPL_15min.png
```

## Methodology mapping

See `docs/methodology_map.md` for explicit labels:

- literature-grounded
- practical implementation choice
- pilot
- future work

## Tests

```bash
pytest -q
```
