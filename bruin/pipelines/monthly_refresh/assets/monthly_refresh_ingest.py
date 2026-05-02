""" @bruin

name: capstone.monthly_refresh_ingest
image: python:3.12

description: |
  Run the existing monthly cloud refresh lane before the downstream dbt step.

@bruin """

from __future__ import annotations

import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[4]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from bruin_runtime import resolve_string, run_pipeline_script


if __name__ == "__main__":
    refresh_command = (
        resolve_string("REFRESH_COMMAND", "refresh_command", default="all-refresh-cloud")
        or "all-refresh-cloud"
    )
    run_pipeline_script(
        refresh_command,
        summary_name="monthly_refresh_ingest",
        context={"refresh_command": refresh_command},
    )
