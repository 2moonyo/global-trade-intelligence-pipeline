""" @bruin

name: capstone.worldbank_energy_bootstrap_full_publish_gcs
image: python:3.12

depends:
  - capstone.worldbank_energy_bootstrap_full_silver

description: |
  Publish the full World Bank energy silver outputs and auxiliary files to GCS.

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
            "warehouse/publish_worldbank_energy_to_gcs.py",
            "--since-year",
            "2015",
            "--until-year",
            "2026",
        ],
        check=True,
        cwd=PROJECT_ROOT,
    )
