""" @bruin

name: capstone.comtrade_bootstrap_day_3_publish_gcs
image: python:3.12

depends:
  - capstone.comtrade_bootstrap_day_3_routing

description: |
  Publish the Comtrade day 3 bronze, silver, metadata, and routing outputs to GCS.

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
            "warehouse/publish_comtrade_to_gcs.py",
            "--since-year-month",
            "2020-01",
            "--until-year-month",
            "2026-12",
        ],
        check=True,
        cwd=PROJECT_ROOT,
    )
