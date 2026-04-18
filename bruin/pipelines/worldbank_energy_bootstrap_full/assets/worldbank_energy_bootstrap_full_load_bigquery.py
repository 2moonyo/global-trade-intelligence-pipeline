""" @bruin

name: capstone.worldbank_energy_bootstrap_full_load_bigquery
image: python:3.12

depends:
  - capstone.worldbank_energy_bootstrap_full_publish_gcs

description: |
  Load the full World Bank energy published partitions into BigQuery.

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
            "warehouse/load_worldbank_energy_to_bigquery.py",
            "--since-year",
            "2015",
            "--until-year",
            "2026",
        ],
        check=True,
        cwd=PROJECT_ROOT,
    )
