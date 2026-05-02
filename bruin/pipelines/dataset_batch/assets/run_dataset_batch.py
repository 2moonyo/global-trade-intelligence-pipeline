""" @bruin

name: capstone.dataset_batch
image: python:3.12

description: |
  Run one manifest-defined dataset batch with ops-ledger writes to Postgres and BigQuery raw ops tables.

@bruin """

from __future__ import annotations

import os
import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[4]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from bruin_runtime import resolve_int, resolve_string, run_pipeline_script


if __name__ == "__main__":
    dataset_name = resolve_string("DATASET_NAME", "dataset_name", required=True)
    batch_id = resolve_string("BATCH_ID", "batch_id", required=True)
    plan_path = resolve_string(
        "BATCH_PLAN_PATH",
        "batch_plan_path",
        default="ops/batch_plan.json",
    )
    pipeline_name = os.getenv("BRUIN_PIPELINE", "capstone.dataset_batch")
    command = [
        "dataset-batch",
        dataset_name,
        batch_id,
        "--plan-path",
        plan_path,
        "--trigger-type",
        "bruin",
        "--bruin-pipeline-name",
        pipeline_name,
    ]
    start_at_task = resolve_string("START_AT_TASK", "start_at_task")
    if start_at_task:
        command.extend(["--start-at-task", start_at_task])

    start_at_step_order = resolve_int("START_AT_STEP_ORDER", "start_at_step_order")
    if start_at_step_order is not None and start_at_step_order > 0:
        command.extend(["--start-at-step-order", str(start_at_step_order)])

    run_pipeline_script(
        *command,
        summary_name="dataset_batch",
        tracked_paths=[plan_path],
        context={
            "batch_id": batch_id,
            "dataset_name": dataset_name,
            "plan_path": plan_path,
            "start_at_step_order": start_at_step_order,
            "start_at_task": start_at_task,
        },
    )
