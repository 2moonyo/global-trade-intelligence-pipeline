""" @bruin

name: capstone.fx_weekly_refresh_extract
image: python:3.12

depends:
  - capstone.fx_weekly_refresh_runtime_gate

description: |
  Extract the recent FX weekly refresh window beginning in 2025-01-01.

@bruin """

from __future__ import annotations

from warehouse.explicit_bruin_assets import run_script_asset


if __name__ == "__main__":
    run_script_asset(
        batch_id="fx_weekly_refresh",
        dataset_name="fx",
        summary_name="fx_weekly_refresh_extract",
        command_args=[
            "python",
            "ingest/fred/fx_rates.py",
            "--start",
            "2025-01-01",
        ],
        tracked_paths=["logs/fx/fx_extract_manifest.jsonl"],
        context={"start": "2025-01-01"},
    )
