""" @bruin

name: capstone.portwatch_bootstrap_phase_1_silver
image: python:3.12

depends:
  - capstone.portwatch_bootstrap_phase_1_extract

description: |
  Build the PortWatch phase 1 silver outputs for the requested bootstrap window.

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
            "ingest/portwatch/portwatch_silver.py",
            "--start-date",
            "2020-01-01",
            "--respect-requested-window",
        ],
        check=True,
        cwd=PROJECT_ROOT,
    )
