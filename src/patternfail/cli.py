from __future__ import annotations

import argparse
import logging

from patternfail.pipeline import run_pipeline


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    ap = argparse.ArgumentParser(description="Run pattern failure dissertation pipeline")
    ap.add_argument("--config", default="configs/default.yaml")
    ap.add_argument(
        "--stage",
        choices=["full", "data", "detect", "experiments"],
        default="full",
        help="Pipeline stage: full end-to-end, data only, detection only, or experiments/reporting only.",
    )
    args = ap.parse_args()
    run_pipeline(args.config, stage=args.stage)


if __name__ == "__main__":
    main()
