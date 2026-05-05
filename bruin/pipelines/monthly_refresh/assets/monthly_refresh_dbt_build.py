""" @bruin

name: capstone.monthly_refresh_dbt_build
image: python:3.12

depends:
  - capstone.monthly_refresh_ingest

description: |
  Run the dbt refresh after the monthly ingestion refresh completes.

@bruin """

from __future__ import annotations

import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[4]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from bruin_runtime import resolve_list, resolve_string, run_dbt_command


if __name__ == "__main__":
    dbt_command = resolve_string("DBT_COMMAND", "dbt_command", default="build") or "build"
    dbt_extra_args = resolve_list("DBT_EXTRA_ARGS", "dbt_extra_args")
    run_dbt_command(
        dbt_command,
        extra_args=dbt_extra_args,
        summary_name="monthly_refresh_dbt_build",
        tracked_paths=[
            "target/manifest.json",
            "target/run_results.json",
            "target/catalog.json",
            "target/graph_summary.json",
            "target/static_index.html",
        ],
        context={
            "dbt_command": dbt_command,
            "dbt_extra_args": dbt_extra_args,
        },
    )
