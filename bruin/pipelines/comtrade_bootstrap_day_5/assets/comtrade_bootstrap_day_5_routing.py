""" @bruin

name: capstone.comtrade_bootstrap_day_5_routing
image: python:3.12

depends:
  - capstone.comtrade_bootstrap_day_5_silver

description: |
  Rebuild the Comtrade routing outputs after the day 5 silver step completes.

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
            "-m",
            "ingest.comtrade.routing",
        ],
        check=True,
        cwd=PROJECT_ROOT,
    )
