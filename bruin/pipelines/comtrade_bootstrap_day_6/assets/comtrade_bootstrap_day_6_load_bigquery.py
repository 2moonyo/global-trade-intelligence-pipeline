""" @bruin

name: capstone.comtrade_bootstrap_day_6_load_bigquery
image: python:3.12

depends:
  - capstone.comtrade_bootstrap_day_6_publish_gcs

description: |
  Load the Comtrade day 6 published assets into BigQuery.

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
            "warehouse/load_comtrade_to_bigquery.py",
            "--since-year-month",
            "2015-01",
            "--until-year-month",
            "2019-12",
        ],
        check=True,
        cwd=PROJECT_ROOT,
    )
