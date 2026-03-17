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
    ap.add_argument("--surrogates-n", type=int, help="Override surrogates.n for quick or heavy runs.")
    ap.add_argument("--max-workers", type=int, help="Max thread workers for surrogate detection loop.")
    ap.add_argument("--progress-every", type=int, help="Log surrogate progress every N jobs.")
    args = ap.parse_args()
    run_pipeline(
        args.config,
        stage=args.stage,
        surrogates_n_override=args.surrogates_n,
        max_workers_override=args.max_workers,
        progress_every_override=args.progress_every,
    )
    args = ap.parse_args()
    run_pipeline(args.config, stage=args.stage)


if __name__ == "__main__":
    main()
