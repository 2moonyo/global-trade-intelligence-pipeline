""" @bruin

name: capstone.portwatch_weekly_refresh_extract
image: python:3.12

depends:
  - capstone.portwatch_weekly_refresh_runtime_gate

description: |
  Extract the recent PortWatch weekly refresh window beginning in 2025-01-01.

@bruin """

from __future__ import annotations

from warehouse.explicit_bruin_assets import run_script_asset


if __name__ == "__main__":
    run_script_asset(
        batch_id="portwatch_weekly_refresh",
        dataset_name="portwatch",
        summary_name="portwatch_weekly_refresh_extract",
        command_args=[
            "python",
            "ingest/portwatch/portwatch_extract.py",
            "--start-date",
            "2025-01-01",
        ],
        tracked_paths=["logs/portwatch/portwatch_extract_weekly_refresh.log"],
        context={"start_date": "2025-01-01"},
    )
