""" @bruin

name: capstone.worldbank_energy_bootstrap_full_silver
image: python:3.12

depends:
  - capstone.worldbank_energy_bootstrap_full_extract

description: |
  Build the World Bank energy silver outputs for the full historical bootstrap window.

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
            "ingest/world_bank/worldbank_energy_silver.py",
            "--start-year",
            "2015",
            "--end-year",
            "2026",
        ],
        check=True,
        cwd=PROJECT_ROOT,
    )
