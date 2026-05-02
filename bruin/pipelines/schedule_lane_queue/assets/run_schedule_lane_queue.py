""" @bruin

name: capstone.schedule_lane_queue
image: python:3.12

description: |
  Run all eligible manifest-defined batches for one schedule lane with retry-aware queue draining.

@bruin """

from __future__ import annotations

import os
import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[4]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from bruin_runtime import resolve_string, run_pipeline_script


if __name__ == "__main__":
    schedule_lane = resolve_string("SCHEDULE_LANE", "schedule_lane", required=True)
    plan_path = resolve_string(
        "BATCH_PLAN_PATH",
        "batch_plan_path",
        default="ops/batch_plan.json",
    )
    pipeline_name = os.getenv("BRUIN_PIPELINE", "capstone.schedule_lane_queue")
    run_pipeline_script(
        "batch-queue",
        schedule_lane,
        "--plan-path",
        plan_path,
        "--trigger-type",
        "bruin",
        "--bruin-pipeline-name",
        pipeline_name,
        summary_name="schedule_lane_queue",
        tracked_paths=[plan_path],
        context={
            "plan_path": plan_path,
            "schedule_lane": schedule_lane,
        },
    )
