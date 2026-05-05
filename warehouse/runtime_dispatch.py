from __future__ import annotations

import os
import subprocess
from dataclasses import asdict, dataclass
from pathlib import Path

from warehouse.execution_profiles import (
    SUPPORTED_RUNTIMES,
    current_runtime,
    current_profile_name,
    runtime_for_batch,
)


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_BRUIN_PIPELINES: dict[str, str] = {
    "portwatch_weekly_refresh": "bruin/pipelines/portwatch_weekly_refresh",
    "brent_weekly_refresh": "bruin/pipelines/brent_weekly_refresh",
    "fx_weekly_refresh": "bruin/pipelines/fx_weekly_refresh",
    "events_incremental_recent": "bruin/pipelines/events_incremental_recent",
}
DEFAULT_CLOUD_RUN_JOB_NAMES: dict[str, str] = {
    "portwatch_weekly_refresh": "capstone-portwatch-weekly",
    "brent_weekly_refresh": "capstone-brent-weekly",
    "fx_weekly_refresh": "capstone-fx-weekly",
    "events_incremental_recent": "capstone-events-incremental",
    "worldbank_energy_yearly_refresh": "capstone-worldbank-energy-yearly",
}


@dataclass(frozen=True)
class DispatchContext:
    batch_id: str
    dataset_name: str
    execution_profile: str
    target_runtime: str
    current_runtime: str
    pipeline_path: str | None
    cloud_run_job_name: str | None


def _normalize_runtime(runtime: str | None) -> str | None:
    if runtime is None:
        return None
    normalized = runtime.strip().lower()
    if not normalized:
        return None
    if normalized not in SUPPORTED_RUNTIMES:
        raise ValueError(
            f"Unsupported target runtime {runtime!r}; supported: {sorted(SUPPORTED_RUNTIMES)}"
        )
    return normalized


def pipeline_path_for_batch(batch_id: str) -> Path | None:
    raw_path = DEFAULT_BRUIN_PIPELINES.get(batch_id)
    if not raw_path:
        return None
    return (PROJECT_ROOT / raw_path).resolve()


def cloud_run_job_name_for_batch(batch_id: str) -> str | None:
    return DEFAULT_CLOUD_RUN_JOB_NAMES.get(batch_id)


def resolve_dispatch_context(
    *,
    batch_id: str,
    dataset_name: str,
    profile_name: str | None = None,
    target_runtime: str | None = None,
    current_runtime_name: str | None = None,
    pipeline_path: str | Path | None = None,
    cloud_run_job_name: str | None = None,
) -> DispatchContext:
    execution_profile = profile_name or current_profile_name()
    resolved_target_runtime = _normalize_runtime(target_runtime) or runtime_for_batch(
        batch_id,
        dataset_name,
        profile_name=execution_profile,
    )
    resolved_current_runtime = _normalize_runtime(current_runtime_name) or current_runtime(default="vm")
    resolved_pipeline_path = (
        str(Path(pipeline_path).resolve())
        if pipeline_path is not None
        else (
            str(pipeline_path_for_batch(batch_id))
            if pipeline_path_for_batch(batch_id) is not None
            else None
        )
    )

    return DispatchContext(
        batch_id=batch_id,
        dataset_name=dataset_name,
        execution_profile=execution_profile,
        target_runtime=resolved_target_runtime,
        current_runtime=resolved_current_runtime,
        pipeline_path=resolved_pipeline_path,
        cloud_run_job_name=cloud_run_job_name or cloud_run_job_name_for_batch(batch_id),
    )


def assert_current_runtime_owner(
    *,
    batch_id: str,
    dataset_name: str,
    profile_name: str | None = None,
    target_runtime: str | None = None,
    current_runtime_name: str | None = None,
) -> dict[str, str]:
    context = resolve_dispatch_context(
        batch_id=batch_id,
        dataset_name=dataset_name,
        profile_name=profile_name,
        target_runtime=target_runtime,
        current_runtime_name=current_runtime_name,
    )
    if context.target_runtime != context.current_runtime:
        raise RuntimeError(
            f"Batch {context.batch_id!r} for dataset {context.dataset_name!r} resolves to runtime "
            f"{context.target_runtime!r} under execution profile {context.execution_profile!r}, "
            f"but this run is executing as {context.current_runtime!r}."
        )
    return {key: str(value) for key, value in asdict(context).items() if value is not None}


def run_local_bruin_pipeline(
    *,
    context: DispatchContext,
    environment: str,
    force: bool = True,
    extra_env: dict[str, str] | None = None,
) -> None:
    if not context.pipeline_path:
        raise ValueError(f"No explicit Bruin pipeline path registered for batch {context.batch_id!r}.")

    env = os.environ.copy()
    env["EXECUTION_PROFILE"] = context.execution_profile
    env["EXECUTION_RUNTIME"] = context.target_runtime
    env["TARGET_RUNTIME"] = context.target_runtime
    if extra_env:
        env.update(extra_env)

    command = ["bruin", "run", "--environment", environment]
    if force:
        command.append("--force")
    command.append(context.pipeline_path)
    subprocess.run(command, check=True, cwd=PROJECT_ROOT, env=env)


def build_cloud_run_execute_command(
    *,
    context: DispatchContext,
    region: str,
    wait: bool,
    bruin_environment: str,
    pipeline_path_override: str | None = None,
) -> list[str]:
    if not context.cloud_run_job_name:
        raise ValueError(f"No Cloud Run job name registered for batch {context.batch_id!r}.")

    command = [
        "gcloud",
        "run",
        "jobs",
        "execute",
        context.cloud_run_job_name,
        "--region",
        region,
    ]
    if wait:
        command.append("--wait")

    args = [
        context.dataset_name,
        context.batch_id,
        "--execution-profile",
        context.execution_profile,
        "--target-runtime",
        context.target_runtime,
        "--bruin-environment",
        bruin_environment,
    ]
    if pipeline_path_override:
        args.extend(["--bruin-pipeline-path", pipeline_path_override])
    command.append(f"--args={','.join(args)}")
    return command
