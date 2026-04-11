""" @bruin

name: capstone.schedule_lane_queue
image: python:3.12

description: |
  Run all eligible manifest-defined batches for one schedule lane with retry-aware queue draining.

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
        raise RuntimeError(f"Missing required environment variable {name} for the Bruin schedule lane asset.")
    return value


if __name__ == "__main__":
    schedule_lane = _required_env("SCHEDULE_LANE")
    plan_path = os.getenv("BATCH_PLAN_PATH", "ops/batch_plan.json")
    subprocess.run(
        [
            str(PIPELINE_SCRIPT),
            "batch-queue",
            schedule_lane,
            "--plan-path",
            plan_path,
            "--trigger-type",
            "bruin",
            "--bruin-pipeline-name",
            "capstone.schedule_lane_queue",
        ],
        check=True,
        cwd=PROJECT_ROOT,
    )
