""" @bruin

name: capstone.events_incremental_recent_publish_gcs
image: python:3.12

depends:
  - capstone.events_incremental_recent_silver

description: |
  Publish the recent Events refresh slice to GCS.

@bruin """

from __future__ import annotations

from warehouse.explicit_bruin_assets import run_script_asset


if __name__ == "__main__":
    run_script_asset(
        batch_id="events_incremental_recent",
        dataset_name="events",
        summary_name="events_incremental_recent_publish_gcs",
        command_args=[
            "python",
            "warehouse/publish_events_to_gcs.py",
            "--since-year-month",
            "2025-01",
        ],
        tracked_paths=["logs/events/publish_events_to_gcs_manifest.jsonl"],
        context={"since_year_month": "2025-01"},
    )
