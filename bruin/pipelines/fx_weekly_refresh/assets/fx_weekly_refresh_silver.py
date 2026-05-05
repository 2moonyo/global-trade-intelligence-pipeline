""" @bruin

name: capstone.fx_weekly_refresh_silver
image: python:3.12

depends:
  - capstone.fx_weekly_refresh_extract

description: |
  Build the FX weekly refresh silver outputs beginning in 2025-01.

@bruin """

from __future__ import annotations

from warehouse.explicit_bruin_assets import run_script_asset


if __name__ == "__main__":
    run_script_asset(
        batch_id="fx_weekly_refresh",
        dataset_name="fx",
        summary_name="fx_weekly_refresh_silver",
        command_args=[
            "python",
            "ingest/fred/fx_silver.py",
            "--since-year-month",
            "2025-01",
        ],
        tracked_paths=["logs/fx/fx_silver_manifest.jsonl"],
        context={"since_year_month": "2025-01"},
    )
