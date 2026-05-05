""" @bruin

name: capstone.portwatch_weekly_refresh_runtime_gate
image: python:3.12

description: |
  Validate that the PortWatch weekly refresh is running on the runtime selected by the execution profile or override.

@bruin """

from __future__ import annotations

from warehouse.explicit_bruin_assets import runtime_gate


if __name__ == "__main__":
    runtime_gate(batch_id="portwatch_weekly_refresh", dataset_name="portwatch")
