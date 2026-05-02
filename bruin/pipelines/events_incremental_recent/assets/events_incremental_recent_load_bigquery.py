""" @bruin

name: capstone.events_incremental_recent_load_bigquery
image: python:3.12

depends:
  - capstone.events_incremental_recent_publish_gcs

description: |
  Load the recent Events refresh partitions into BigQuery.

@bruin """

from __future__ import annotations

from warehouse.explicit_bruin_assets import run_script_asset


if __name__ == "__main__":
    run_script_asset(
        batch_id="events_incremental_recent",
        dataset_name="events",
        summary_name="events_incremental_recent_load_bigquery",
        command_args=[
            "python",
            "warehouse/load_events_to_bigquery.py",
            "--since-year-month",
            "2025-01",
        ],
        tracked_paths=["logs/events/load_events_to_bigquery_manifest.jsonl"],
        context={"since_year_month": "2025-01"},
    )
