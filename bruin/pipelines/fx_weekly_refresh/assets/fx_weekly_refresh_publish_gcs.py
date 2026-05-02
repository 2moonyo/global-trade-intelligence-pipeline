""" @bruin

name: capstone.fx_weekly_refresh_publish_gcs
image: python:3.12

depends:
  - capstone.fx_weekly_refresh_silver

description: |
  Publish the FX weekly refresh slice to GCS.

@bruin """

from __future__ import annotations

from warehouse.explicit_bruin_assets import run_script_asset


if __name__ == "__main__":
    run_script_asset(
        batch_id="fx_weekly_refresh",
        dataset_name="fx",
        summary_name="fx_weekly_refresh_publish_gcs",
        command_args=[
            "python",
            "warehouse/publish_fx_to_gcs.py",
            "--since-year-month",
            "2025-01",
        ],
        tracked_paths=["logs/fx/publish_fx_to_gcs_manifest.jsonl"],
        context={"since_year_month": "2025-01"},
    )
