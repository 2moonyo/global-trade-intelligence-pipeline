""" @bruin

name: capstone.portwatch_weekly_refresh_publish_gcs
image: python:3.12

depends:
  - capstone.portwatch_weekly_refresh_silver

description: |
  Publish the PortWatch weekly refresh slice and auxiliary files to GCS.

@bruin """

from __future__ import annotations

from warehouse.explicit_bruin_assets import run_script_asset


if __name__ == "__main__":
    run_script_asset(
        batch_id="portwatch_weekly_refresh",
        dataset_name="portwatch",
        summary_name="portwatch_weekly_refresh_publish_gcs",
        command_args=[
            "python",
            "warehouse/publish_portwatch_to_gcs.py",
            "--include-auxiliary",
            "--since-year-month",
            "2025-01",
        ],
        tracked_paths=["logs/portwatch/publish_portwatch_to_gcs_manifest.jsonl"],
        context={"since_year_month": "2025-01"},
    )
