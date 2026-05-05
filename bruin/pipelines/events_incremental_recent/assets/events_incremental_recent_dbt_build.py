""" @bruin

name: capstone.events_incremental_recent_dbt_build
uri: capstone://batch/events_incremental_recent/dbt_build
image: python:3.12

depends:
  - capstone.events_incremental_recent_load_bigquery

description: |
  Run dbt after the Events incremental refresh BigQuery load completes.

@bruin """

from __future__ import annotations

from warehouse.explicit_bruin_assets import run_dbt_asset


if __name__ == "__main__":
    run_dbt_asset(
        batch_id="events_incremental_recent",
        dataset_name="events",
        summary_name="events_incremental_recent_dbt_build",
        tracked_paths=[
            "target/manifest.json",
            "target/run_results.json",
            "target/catalog.json",
        ],
    )
