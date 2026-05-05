""" @bruin

name: capstone.fx_bootstrap_phase_1_extract
image: python:3.12

description: |
  Extract the FX bootstrap phase 1 bronze window beginning in 2020-01-01.

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
            "ingest/fred/fx_rates.py",
            "--start",
            "2020-01-01",
        ],
        check=True,
        cwd=PROJECT_ROOT,
    )
