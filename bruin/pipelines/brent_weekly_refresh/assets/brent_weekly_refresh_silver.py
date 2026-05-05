""" @bruin

name: capstone.brent_weekly_refresh_silver
image: python:3.12

depends:
  - capstone.brent_weekly_refresh_extract

description: |
  Build the Brent weekly refresh silver outputs.

@bruin """

from __future__ import annotations

from warehouse.explicit_bruin_assets import run_script_asset


if __name__ == "__main__":
    run_script_asset(
        batch_id="brent_weekly_refresh",
        dataset_name="brent",
        summary_name="brent_weekly_refresh_silver",
        command_args=[
            "python",
            "ingest/fred/brent_silver.py",
        ],
        tracked_paths=["logs/brent/brent_silver_manifest.jsonl"],
    )
