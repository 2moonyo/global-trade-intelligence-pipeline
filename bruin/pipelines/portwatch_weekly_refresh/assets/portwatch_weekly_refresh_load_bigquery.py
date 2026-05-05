""" @bruin

name: capstone.portwatch_weekly_refresh_load_bigquery
image: python:3.12

depends:
  - capstone.portwatch_weekly_refresh_publish_gcs

description: |
  Load the PortWatch weekly refresh partitions into BigQuery.

@bruin """

from __future__ import annotations

from warehouse.explicit_bruin_assets import run_script_asset


if __name__ == "__main__":
    run_script_asset(
        batch_id="portwatch_weekly_refresh",
        dataset_name="portwatch",
        summary_name="portwatch_weekly_refresh_load_bigquery",
        command_args=[
            "python",
            "warehouse/load_portwatch_to_bigquery.py",
            "--since-year-month",
            "2025-01",
        ],
        tracked_paths=["logs/portwatch/load_portwatch_to_bigquery_manifest.jsonl"],
        context={"since_year_month": "2025-01"},
    )
