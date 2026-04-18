""" @bruin

name: capstone.fx_bootstrap_phase_1_load_bigquery
image: python:3.12

depends:
  - capstone.fx_bootstrap_phase_1_publish_gcs

description: |
  Load the FX phase 1 published partitions into BigQuery.

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
            "warehouse/load_fx_to_bigquery.py",
            "--since-year-month",
            "2020-01",
        ],
        check=True,
        cwd=PROJECT_ROOT,
    )
