""" @bruin

name: capstone.portwatch_weekly_refresh_silver
image: python:3.12

depends:
  - capstone.portwatch_weekly_refresh_extract

description: |
  Build the PortWatch silver refresh outputs for the requested weekly window.

@bruin """

from __future__ import annotations

from warehouse.explicit_bruin_assets import run_script_asset


if __name__ == "__main__":
    run_script_asset(
        batch_id="portwatch_weekly_refresh",
        dataset_name="portwatch",
        summary_name="portwatch_weekly_refresh_silver",
        command_args=[
            "python",
            "ingest/portwatch/portwatch_silver.py",
            "--start-date",
            "2025-01-01",
            "--respect-requested-window",
        ],
        tracked_paths=["logs/portwatch/portwatch_silver_manifest.jsonl"],
        context={"start_date": "2025-01-01"},
    )
