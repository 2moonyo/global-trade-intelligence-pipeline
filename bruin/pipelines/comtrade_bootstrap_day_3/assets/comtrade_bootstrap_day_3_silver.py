""" @bruin

name: capstone.comtrade_bootstrap_day_3_silver
image: python:3.12

depends:
  - capstone.comtrade_bootstrap_day_3_extract

description: |
  Build the Comtrade day 3 silver outputs for the encoded reporter set and current-year window.

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
            "1001,1005,1006,1201,2709,2710",
            "--flow-codes",
            "M,X",
        ],
        check=True,
        cwd=PROJECT_ROOT,
    )
