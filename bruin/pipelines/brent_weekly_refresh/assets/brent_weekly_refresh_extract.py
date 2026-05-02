""" @bruin

name: capstone.brent_weekly_refresh_extract
image: python:3.12

depends:
  - capstone.brent_weekly_refresh_runtime_gate

description: |
  Extract the recent Brent weekly refresh window beginning in 2025-01-01.

@bruin """

from __future__ import annotations

from warehouse.explicit_bruin_assets import run_script_asset


if __name__ == "__main__":
    run_script_asset(
        batch_id="brent_weekly_refresh",
        dataset_name="brent",
        summary_name="brent_weekly_refresh_extract",
        command_args=[
            "python",
            "ingest/fred/brent_crude.py",
            "--start",
            "2025-01-01",
        ],
        tracked_paths=["logs/brent/brent_extract_manifest.jsonl"],
        context={"start": "2025-01-01"},
    )
