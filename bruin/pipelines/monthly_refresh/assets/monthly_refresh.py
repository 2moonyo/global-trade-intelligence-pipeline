""" @bruin

name: capstone.monthly_refresh
image: python:3.12

description: |
  Run the existing cloud refresh workflows in sequence and then build the dbt project in BigQuery.

@bruin """

from __future__ import annotations

import subprocess
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[4]
PIPELINE_SCRIPT = PROJECT_ROOT / "scripts" / "run_pipeline.sh"
DBT_SCRIPT = PROJECT_ROOT / "scripts" / "run_dbt.sh"


def _run(*args: str) -> None:
    subprocess.run([str(arg) for arg in args], check=True, cwd=PROJECT_ROOT)


if __name__ == "__main__":
    _run(PIPELINE_SCRIPT, "all-refresh-cloud")
    _run(DBT_SCRIPT, "build")
