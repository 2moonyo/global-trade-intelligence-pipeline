from __future__ import annotations

import argparse
import os
import subprocess
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from warehouse.batch_plan import load_batch_plan
from warehouse.runtime_dispatch import (
    build_cloud_run_execute_command,
    resolve_dispatch_context,
    run_local_bruin_pipeline,
)


def _build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Resolve runtime ownership for an explicit Bruin pipeline and run it locally or dispatch to Cloud Run."
    )
    parser.add_argument("--batch-id", required=True, help="Batch id from ops/batch_plan.json.")
    parser.add_argument("--plan-path", default=None, help="Optional batch plan path.")
    parser.add_argument("--execution-profile", default=None, help="Optional execution profile override.")
    parser.add_argument("--target-runtime", default=None, help="Optional target runtime override: vm or cloud_run.")
    parser.add_argument("--current-runtime", default=None, help="Optional current runtime override for local execution.")
    parser.add_argument("--pipeline-path", default=None, help="Optional explicit Bruin pipeline path override.")
    parser.add_argument("--cloud-run-job-name", default=None, help="Optional Cloud Run job name override.")
    parser.add_argument("--cloud-run-region", default=None, help="Cloud Run region for dispatched jobs.")
    parser.add_argument("--environment", default="production", help="Bruin environment name for local runs.")
    parser.add_argument(
        "--cloud-run-environment",
        default="production",
        help="Bruin environment name passed through when dispatching to Cloud Run.",
    )
    parser.add_argument("--wait", action="store_true", help="Wait for the Cloud Run Job execution to finish.")
    return parser


def main() -> None:
    args = _build_arg_parser().parse_args()
    plan = load_batch_plan(args.plan_path)
    batch = plan.get(args.batch_id)
    if batch is None:
        raise SystemExit(f"Unknown batch_id {args.batch_id!r} in batch plan.")

    context = resolve_dispatch_context(
        batch_id=batch.batch_id,
        dataset_name=batch.dataset_name,
        profile_name=args.execution_profile,
        target_runtime=args.target_runtime,
        current_runtime_name=args.current_runtime,
        pipeline_path=args.pipeline_path,
        cloud_run_job_name=args.cloud_run_job_name,
    )

    if context.target_runtime == "cloud_run" and context.current_runtime != "cloud_run":
        region = args.cloud_run_region or os.getenv("SERVERLESS_REGION") or os.getenv("CLOUD_RUN_REGION")
        if not region:
            raise SystemExit(
                "Cloud Run dispatch requires --cloud-run-region or SERVERLESS_REGION/CLOUD_RUN_REGION."
            )
        command = build_cloud_run_execute_command(
            context=context,
            region=region,
            wait=args.wait,
            bruin_environment=args.cloud_run_environment,
            pipeline_path_override=context.pipeline_path,
        )
        subprocess.run(command, check=True, cwd=PROJECT_ROOT)
        return

    run_local_bruin_pipeline(
        context=context,
        environment=args.environment,
        extra_env={"EXECUTION_RUNTIME": context.target_runtime, "TARGET_RUNTIME": context.target_runtime},
    )


if __name__ == "__main__":
    main()
