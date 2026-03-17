from __future__ import annotations

from pathlib import Path


def ensure_run_dirs(output_root: str, run_name: str) -> dict[str, Path]:
    root = Path(output_root) / run_name
    dirs = {
        "root": root,
        "clean": root / "clean_1m",
        "bars": root / "bars",
        "turning_points": root / "turning_points",
        "patterns": root / "patterns",
        "outcomes": root / "outcomes",
        "significance": root / "significance",
        "experiments": root / "experiments",
        "figures": root / "figures",
        "meta": root / "meta",
    }
    for p in dirs.values():
        p.mkdir(parents=True, exist_ok=True)
    return dirs
