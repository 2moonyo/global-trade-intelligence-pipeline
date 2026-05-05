""" @bruin

name: capstone.worldbank_energy_bootstrap_full_extract
image: python:3.12

depends:
  - uri: capstone://batch/comtrade_bootstrap_day_6/dbt_build

description: |
  Extract the full World Bank energy bootstrap window from 2015 through 2026.

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
            "ingest/world_bank/worldbank_energy.py",
            "extract",
            "--selector",
            "db-countries",
            "--energy-types",
            "all",
            "--start-year",
            "2015",
            "--end-year",
            "2026",
        ],
        check=True,
        cwd=PROJECT_ROOT,
    )
