""" @bruin

name: capstone.dataset_batch
image: python:3.12

description: |
  Run one manifest-defined dataset batch with ops-ledger writes to Postgres and BigQuery raw ops tables.

@bruin """

from __future__ import annotations

import os
import subprocess
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[4]
PIPELINE_SCRIPT = PROJECT_ROOT / "scripts" / "run_pipeline.sh"


def _required_env(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise RuntimeError(f"Missing required environment variable {name} for the Bruin dataset batch asset.")
    return value


if __name__ == "__main__":
    dataset_name = _required_env("DATASET_NAME")
    batch_id = _required_env("BATCH_ID")
    plan_path = os.getenv("BATCH_PLAN_PATH", "ops/batch_plan.json")
    command = [
        str(PIPELINE_SCRIPT),
        "dataset-batch",
        dataset_name,
        batch_id,
        "--plan-path",
        plan_path,
        "--trigger-type",
        "bruin",
        "--bruin-pipeline-name",
        "capstone.dataset_batch",
    ]
    start_at_task = os.getenv("START_AT_TASK")
    if start_at_task:
        command.extend(["--start-at-task", start_at_task])

    start_at_step_order = os.getenv("START_AT_STEP_ORDER")
    if start_at_step_order:
        command.extend(["--start-at-step-order", start_at_step_order])

    subprocess.run(command, check=True, cwd=PROJECT_ROOT)
