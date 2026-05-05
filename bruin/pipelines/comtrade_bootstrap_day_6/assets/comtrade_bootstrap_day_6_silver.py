""" @bruin

name: capstone.comtrade_bootstrap_day_6_silver
image: python:3.12

depends:
  - capstone.comtrade_bootstrap_day_6_extract

description: |
  Build the Comtrade day 6 silver outputs for the expanded historic reporter and energy-commodity window.

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
            "201501",
            "--until-period",
            "201912",
            "--cmd-codes",
            "2711,3102,3105",
            "--flow-codes",
            "M,X",
        ],
        check=True,
        cwd=PROJECT_ROOT,
    )
