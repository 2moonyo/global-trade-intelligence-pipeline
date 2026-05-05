""" @bruin

name: capstone.brent_bootstrap_phase_1_silver
image: python:3.12

depends:
  - capstone.brent_bootstrap_phase_1_extract

description: |
  Build the Brent phase 1 silver outputs for the live bootstrap window.

@bruin """

from __future__ import annotations

import subprocess
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[4]
PIPELINE_SCRIPT = PROJECT_ROOT / "scripts" / "run_pipeline.sh"


if __name__ == "__main__":
    subprocess.run(
        [
            str(PIPELINE_SCRIPT),
            "python",
            "ingest/fred/brent_silver.py",
        ],
        check=True,
        cwd=PROJECT_ROOT,
    )
