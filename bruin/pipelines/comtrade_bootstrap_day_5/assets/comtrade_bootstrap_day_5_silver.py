""" @bruin

name: capstone.comtrade_bootstrap_day_5_silver
image: python:3.12

depends:
  - capstone.comtrade_bootstrap_day_5_extract

description: |
  Build the Comtrade day 5 silver outputs for the expanded reporter and energy-commodity window.

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
            "ingest/comtrade/comtrade_silver.py",
            "--since-period",
            "202001",
            "--until-period",
            "202612",
            "--cmd-codes",
            "2711,3102,3105",
            "--flow-codes",
            "M,X",
        ],
        check=True,
        cwd=PROJECT_ROOT,
    )
