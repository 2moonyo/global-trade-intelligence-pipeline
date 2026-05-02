""" @bruin

name: capstone.events_incremental_recent_silver
image: python:3.12

depends:
  - capstone.events_incremental_recent_runtime_gate

description: |
  Build the recent Events silver outputs for the incremental refresh window.

@bruin """

from __future__ import annotations

from warehouse.explicit_bruin_assets import run_script_asset


if __name__ == "__main__":
    run_script_asset(
        batch_id="events_incremental_recent",
        dataset_name="events",
        summary_name="events_incremental_recent_silver",
        command_args=[
            "python",
            "ingest/events/events_silver.py",
        ],
        tracked_paths=["logs/events/events_silver_manifest.jsonl"],
    )
