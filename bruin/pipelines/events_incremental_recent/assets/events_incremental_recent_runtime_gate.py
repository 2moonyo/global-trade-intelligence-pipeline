""" @bruin

name: capstone.events_incremental_recent_runtime_gate
image: python:3.12

description: |
  Validate that the Events incremental refresh is running on the runtime selected by the execution profile or override.

@bruin """

from __future__ import annotations

from warehouse.explicit_bruin_assets import runtime_gate


if __name__ == "__main__":
    runtime_gate(batch_id="events_incremental_recent", dataset_name="events")
