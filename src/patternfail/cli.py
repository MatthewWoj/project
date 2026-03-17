from __future__ import annotations

import argparse
import logging

from patternfail.pipeline import run_pipeline


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    ap = argparse.ArgumentParser(description="Run pattern failure dissertation pipeline")
    ap.add_argument("--config", default="configs/default.yaml")
    args = ap.parse_args()
    run_pipeline(args.config)


if __name__ == "__main__":
    main()
