""" @bruin

name: capstone.comtrade_bootstrap_day_1_metadata
image: python:3.12

depends:
  - capstone.comtrade_bootstrap_day_1_extract

description: |
  Refresh the Comtrade reference metadata used by the day 1 silver and routing steps.

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
            "ingest/comtrade/un_comtrade_tools_metadata.py",
        ],
        check=True,
        cwd=PROJECT_ROOT,
    )
