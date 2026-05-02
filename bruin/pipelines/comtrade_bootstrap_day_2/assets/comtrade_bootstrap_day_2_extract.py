""" @bruin

name: capstone.comtrade_bootstrap_day_2_extract
image: python:3.12

depends:
  - uri: capstone://batch/comtrade_bootstrap_day_1/dbt_build

description: |
  Run the self-contained Comtrade bootstrap day 2 monthly-history extraction lane.

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
            "ingest/comtrade/comtrade_cli_annual_monthly_gap_chunked_by_reporter.py",
            "run-monthly-history",
            "--years",
            "2015,2016,2017,2018,2019",
            "--reporters",
            "97,100,156,251,528,642,724,842,643,699,710,818,792,360,076,591",
            "--commodities",
            "1001,1005,1006,1201,2709,2710",
            "--flows",
            "M,X",
            "--sleep-on-quota",
            "--registry-path",
            "data/metadata/comtrade/state/extraction_registry.jsonl",
            "--checkpoint-path",
            "data/metadata/comtrade/state/comtrade_checkpoint.json",
        ],
        check=True,
        cwd=PROJECT_ROOT,
    )
