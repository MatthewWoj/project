from __future__ import annotations

import argparse
import json
from pathlib import Path

import pandas as pd


JSON_COLUMNS = {"geometry_params", "context_labels", "outcome_labels"}


def _read_table(path: Path) -> pd.DataFrame:
    if path.suffix == ".parquet" and path.exists():
        return pd.read_parquet(path)
    alt = path.with_suffix(".csv")
    if alt.exists():
        return pd.read_csv(alt)
    raise FileNotFoundError(path)


def _resolve_table(run_root: Path, table: str) -> Path:
    direct = run_root / table
    if direct.exists():
        return direct
    if direct.with_suffix(".parquet").exists() or direct.with_suffix(".csv").exists():
        return direct.with_suffix(".parquet")
    for ext in (".parquet", ".csv"):
        candidate = run_root / f"{table}{ext}"
        if candidate.exists():
            return candidate
    raise FileNotFoundError(table)


def _maybe_parse_json(value):
    if isinstance(value, str):
        value = value.strip()
        if value.startswith("{"):
            try:
                return json.loads(value)
            except json.JSONDecodeError:
                return value
    return value


def _expand_json_cols(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    for col in list(out.columns):
        if col not in JSON_COLUMNS:
            continue
        out[col] = out[col].apply(_maybe_parse_json)
        sample = next((v for v in out[col] if isinstance(v, dict) and v), None)
        if not sample:
            continue
        expanded = pd.json_normalize(out[col]).add_prefix(f"{col}.")
        out = pd.concat([out.drop(columns=[col]), expanded], axis=1)
    return out


def _list_outputs(run_root: Path) -> pd.DataFrame:
    rows = []
    for path in sorted(p for p in run_root.rglob("*") if p.is_file()):
        rows.append(
            {
                "relative_path": str(path.relative_to(run_root)),
                "format": path.suffix.lstrip("."),
                "size_kb": round(path.stat().st_size / 1024, 1),
            }
        )
    return pd.DataFrame(rows)


def _summarize_patterns(df: pd.DataFrame) -> pd.DataFrame:
    cols = [c for c in ["asset", "timeframe", "pattern_type", "direction"] if c in df.columns]
    if not cols:
        return pd.DataFrame()
    out = df.groupby(cols).size().rename("n").reset_index().sort_values("n", ascending=False)
    return out


def _summarize_outcomes(df: pd.DataFrame) -> pd.DataFrame:
    work = _expand_json_cols(df)
    if "outcome_labels.status" not in work.columns:
        return pd.DataFrame()
    group_cols = [c for c in ["asset", "timeframe", "pattern_type", "outcome_labels.status"] if c in work.columns]
    out = work.groupby(group_cols).size().rename("n").reset_index().sort_values("n", ascending=False)
    return out


def _print_df(df: pd.DataFrame, limit: int) -> None:
    if df.empty:
        print("<empty>")
        return
    with pd.option_context("display.max_columns", None, "display.width", 200, "display.max_colwidth", 80):
        print(df.head(limit).to_string(index=False))


def main() -> None:
    ap = argparse.ArgumentParser(description="Inspect generated patternfail output tables without needing a parquet GUI.")
    ap.add_argument("--run-root", required=True, help="Path like data/outputs/user_btc_nvda")
    ap.add_argument("--list", action="store_true", help="List all produced files under the run root.")
    ap.add_argument("--table", help="Relative table path, with or without .parquet/.csv suffix.")
    ap.add_argument("--head", type=int, default=20, help="Number of rows to print.")
    ap.add_argument("--columns", nargs="*", help="Optional subset of columns to display.")
    ap.add_argument("--expand-json", action="store_true", help="Expand geometry/context/outcome JSON columns into flat columns.")
    ap.add_argument("--summary", choices=["patterns", "outcomes"], help="Print a useful aggregate summary for a loaded table.")
    args = ap.parse_args()

    run_root = Path(args.run_root)

    if args.list:
        print("=== files ===")
        _print_df(_list_outputs(run_root), limit=10_000)

    if args.table:
        path = _resolve_table(run_root, args.table)
        df = _read_table(path)
        if args.expand_json:
            df = _expand_json_cols(df)
        if args.columns:
            df = df.loc[:, [c for c in args.columns if c in df.columns]]
        print(f"=== table: {path.relative_to(run_root)} ===")
        print(f"rows={len(df)} cols={len(df.columns)}")
        _print_df(df, args.head)
        if args.summary == "patterns":
            print("\n=== pattern summary ===")
            _print_df(_summarize_patterns(df), args.head)
        if args.summary == "outcomes":
            print("\n=== outcome summary ===")
            _print_df(_summarize_outcomes(df), args.head)

    if not args.list and not args.table:
        ap.error("Provide at least --list or --table")


if __name__ == "__main__":
    main()
