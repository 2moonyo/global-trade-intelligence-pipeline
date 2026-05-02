""" @bruin

name: capstone.brent_weekly_refresh_dbt_build
uri: capstone://batch/brent_weekly_refresh/dbt_build
image: python:3.12

depends:
  - capstone.brent_weekly_refresh_load_bigquery

description: |
  Run dbt after the Brent weekly refresh BigQuery load completes.

@bruin """

from __future__ import annotations

from warehouse.explicit_bruin_assets import run_dbt_asset


if __name__ == "__main__":
    run_dbt_asset(
        batch_id="brent_weekly_refresh",
        dataset_name="brent",
        summary_name="brent_weekly_refresh_dbt_build",
        tracked_paths=[
            "target/manifest.json",
            "target/run_results.json",
            "target/catalog.json",
        ],
    )
