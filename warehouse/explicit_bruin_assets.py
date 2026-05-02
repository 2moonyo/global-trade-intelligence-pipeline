from __future__ import annotations

from pathlib import Path
import sys

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from bruin_runtime import resolve_string, run_dbt_command, run_pipeline_script
from warehouse.runtime_dispatch import assert_current_runtime_owner


def _runtime_context(*, batch_id: str, dataset_name: str) -> dict[str, str]:
    execution_profile = resolve_string("EXECUTION_PROFILE", "execution_profile")
    target_runtime = resolve_string("TARGET_RUNTIME", "target_runtime")
    return assert_current_runtime_owner(
        batch_id=batch_id,
        dataset_name=dataset_name,
        profile_name=execution_profile,
        target_runtime=target_runtime,
    )


def runtime_gate(*, batch_id: str, dataset_name: str) -> dict[str, str]:
    return _runtime_context(batch_id=batch_id, dataset_name=dataset_name)


def run_script_asset(
    *,
    batch_id: str,
    dataset_name: str,
    summary_name: str,
    command_args: list[str],
    tracked_paths: list[str] | None = None,
    context: dict[str, object] | None = None,
) -> None:
    runtime_context = _runtime_context(batch_id=batch_id, dataset_name=dataset_name)
    run_pipeline_script(
        *command_args,
        summary_name=summary_name,
        tracked_paths=tracked_paths,
        context={**runtime_context, **(context or {})},
    )


def run_dbt_asset(
    *,
    batch_id: str,
    dataset_name: str,
    summary_name: str,
    dbt_command: str = "build",
    tracked_paths: list[str] | None = None,
    context: dict[str, object] | None = None,
) -> None:
    runtime_context = _runtime_context(batch_id=batch_id, dataset_name=dataset_name)
    run_dbt_command(
        dbt_command,
        summary_name=summary_name,
        tracked_paths=tracked_paths,
        context={**runtime_context, **(context or {})},
    )
