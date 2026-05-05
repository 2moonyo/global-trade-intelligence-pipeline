""" @bruin

name: capstone.brent_bootstrap_phase_1_publish_gcs
image: python:3.12

depends:
  - capstone.brent_bootstrap_phase_1_silver

description: |
  Publish the Brent phase 1 silver outputs and auxiliary files to GCS.

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
            "warehouse/publish_brent_to_gcs.py",
            "--since-year-month",
            "2020-01",
        ],
        check=True,
        cwd=PROJECT_ROOT,
    )
