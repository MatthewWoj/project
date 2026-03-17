# CSV Schema and Timestamp Handling

## Required logical fields
Each input CSV must provide these logical fields:

- timestamp
- open
- high
- low
- close
- volume

Use `csv.cols` and `csv.timestamp_col` to map your raw column names to these logical fields.

Use `csv.separator` (or `csv.asset_overrides.<ASSET>.separator`) for delimiter differences such as comma vs tab.

## Expected data rules

- `timestamp` must parse via `pandas.to_datetime`.
- OHLCV values are converted with `pandas.to_numeric(errors="coerce")`.
- Rows are dropped when OHLC is invalid (`low` above open/close, `high` below open/close, or missing OHLC).
- Duplicate timestamps keep the last row.

## Naive timestamp interpretation and UTC conversion

- If timestamps are naive (no timezone info), they are interpreted in `csv.assume_tz` (or `csv.asset_overrides.<ASSET>.assume_tz`).
- After localization, timestamps are converted to UTC and stored as `ts_utc`.
- If timestamps are already timezone-aware, they are converted directly to UTC.

This gives one canonical internal timeline across equities, FX, metals, and crypto.

## Flexible per-asset mapping

Use `csv.asset_overrides` when schemas differ across files.

Example:

```yaml
csv:
  timestamp_col: timestamp
  assume_tz: UTC
  cols: {open: open, high: high, low: low, close: close, volume: volume}
  asset_overrides:
    EURUSD:
      timestamp_col: DateTime
      separator: ","
      assume_tz: Europe/London
      cols: {open: Open, high: High, low: Low, close: Close, volume: TickVolume}
```
