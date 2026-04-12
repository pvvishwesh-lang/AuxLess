
"""
DVC setup and reproduction helper for the Auxless streaming pipeline.

Usage:
    python dvc_setup.py --init       # initialise DVC in the repo (run once)
    python dvc_setup.py --repro      # run dvc repro
    python dvc_setup.py --metrics    # print tracked metrics
"""

import argparse
import json
import logging
import os
import subprocess
import sys
from pathlib import Path

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")

METRICS_DIR = Path("metrics")
METRICS_FILE = METRICS_DIR / "pipeline_metrics.json"


def _run(cmd: list[str], check: bool = True) -> subprocess.CompletedProcess:
    logger.info("Running: %s", " ".join(cmd))
    return subprocess.run(cmd, check=check, capture_output=False, text=True)


def init_dvc() -> None:
    """Initialise DVC if not already initialised."""
    if Path(".dvc").exists():
        logger.info(".dvc/ already exists — skipping init.")
        return
    _run(["dvc", "init"])
    logger.info("DVC initialised successfully.")


def ensure_metrics_dir() -> None:
    """Create the metrics output directory expected by dvc.yaml."""
    METRICS_DIR.mkdir(exist_ok=True)
    if not METRICS_FILE.exists():
        METRICS_FILE.write_text(json.dumps({
            "window_size_seconds": int(os.environ.get("WINDOW_SIZE_SECONDS", 60)),
            "runner": "DataflowRunner",
            "note": "Populated at pipeline run time.",
        }, indent=2))
        logger.info("Stub metrics file created at %s", METRICS_FILE)


def repro() -> None:
    """Run dvc repro to execute the pipeline stage."""
    ensure_metrics_dir()
    _run(["dvc", "repro"])


def show_metrics() -> None:
    """Print current DVC-tracked metrics."""
    _run(["dvc", "metrics", "show"], check=False)


def main() -> None:
    parser = argparse.ArgumentParser(description="DVC pipeline helper")
    parser.add_argument("--init",    action="store_true", help="Initialise DVC")
    parser.add_argument("--repro",   action="store_true", help="Run dvc repro")
    parser.add_argument("--metrics", action="store_true", help="Show metrics")
    args = parser.parse_args()

    if not any(vars(args).values()):
        parser.print_help()
        sys.exit(0)

    if args.init:
        init_dvc()
    if args.repro:
        repro()
    if args.metrics:
        show_metrics()


if __name__ == "__main__":
    main()