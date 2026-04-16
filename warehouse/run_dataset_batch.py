from __future__ import annotations

import argparse
import json
import os
import re
import shutil
import subprocess
import sys
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from ingest.common.cloud_config import GcpCloudConfig
from ingest.common.run_artifacts import (
    build_run_id,
    configure_logger,
    duration_seconds,
    json_ready,
)
from warehouse.batch_plan import BatchDefinition, BatchStep, load_batch_plan, resolve_batch_plan_path
from warehouse.ops_store import (
    BigQueryOpsMirror,
    PostgresOpsStore,
    bigquery_mirror_enabled,
)


PARTITION_PATTERNS = (
    re.compile(r"year=(\d{4})/month=(\d{2})"),
    re.compile(r"dt=(\d{4}-\d{2}-\d{2})"),
)


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _strict_bigquery_mirror() -> bool:
    return os.getenv("OPS_STRICT_BIGQUERY_MIRROR", "false").strip().lower() in {"1", "true", "yes"}


def _safe_cloud_config() -> GcpCloudConfig | None:
    try:
        return GcpCloudConfig.from_env()
    except Exception:
        return None


def _default_pipeline_log_path(batch: BatchDefinition, pipeline_run_id: str, attempt_number: int) -> Path:
    return (
        PROJECT_ROOT
        / "logs"
        / "ops"
        / batch.dataset_name
        / batch.batch_id
        / f"attempt={attempt_number}"
        / f"{pipeline_run_id}.log"
    )


def _default_task_log_path(batch: BatchDefinition, task_name: str, step_order: int, attempt_number: int) -> Path:
    safe_task_name = task_name.replace(" ", "_")
    return (
        PROJECT_ROOT
        / "logs"
        / "ops"
        / batch.dataset_name
        / batch.batch_id
        / f"attempt={attempt_number}"
        / f"{step_order:02d}_{safe_task_name}.log"
    )


def _manifest_size(path: Path | None) -> int:
    if path is None or not path.exists():
        return 0
    return path.stat().st_size


def _read_manifest_entries_since(path: Path | None, size_before: int) -> list[dict[str, Any]]:
    if path is None or not path.exists():
        return []

    entries: list[dict[str, Any]] = []
    with path.open("rb") as handle:
        if size_before > 0:
            handle.seek(size_before)
        for line in handle:
            if not line.strip():
                continue
            entries.append(json.loads(line.decode("utf-8")))
    return entries


def _resolve_project_path(path_text: str | None) -> Path | None:
    if not path_text:
        return None
    candidate = Path(path_text)
    return candidate if candidate.is_absolute() else (PROJECT_ROOT / candidate).resolve()


def _normalize_command(command: tuple[str, ...]) -> list[str]:
    if not command:
        raise ValueError("Batch step command cannot be empty")

    first = command[0]
    if first.endswith(".sh"):
        script_path = _resolve_project_path(first)
        return ["bash", str(script_path), *command[1:]]
    if first.endswith(".py"):
        script_path = _resolve_project_path(first)
        return [sys.executable, str(script_path), *command[1:]]
    return list(command)


def _detect_partition_key(value: str | None) -> str | None:
    if not value:
        return None
    for pattern in PARTITION_PATTERNS:
        match = pattern.search(value)
        if not match:
            continue
        if len(match.groups()) == 2:
            return f"{match.group(1)}-{match.group(2)}"
        return match.group(1)
    return None


def _task_metrics(step: BatchStep, manifest_entry: dict[str, Any] | None) -> dict[str, Any]:
    metrics = {
        "step_notes": step.notes,
    }
    if not manifest_entry:
        return metrics

    interesting_keys = (
        "status",
        "duration_seconds",
        "touched_year_months",
        "selected_year_months",
        "requested_year_months",
        "requested_periods",
        "rows_monthly",
        "source_row_count",
        "rows_after_required_field_filter",
        "fact_slices_written",
        "fact_slices_skipped_unchanged",
        "candidate_year_months",
        "skipped_loaded_year_months",
        "output_rows",
    )
    for key in interesting_keys:
        if key in manifest_entry:
            metrics[key] = manifest_entry[key]
    if "uploads" in manifest_entry:
        metrics["upload_specs"] = sorted(manifest_entry["uploads"].keys())
    return metrics


def _task_artifact_row(
    *,
    pipeline_run_id: str,
    task_run_id: str,
    batch: BatchDefinition,
    artifact_type: str,
    direction: str | None = None,
    local_path: str | None = None,
    gcs_uri: str | None = None,
    load_batch_id: str | None = None,
    source_file: str | None = None,
    partition_key: str | None = None,
    checksum: str | None = None,
    record_count: int | None = None,
    payload_json: dict[str, Any] | None = None,
) -> dict[str, Any]:
    return {
        "task_artifact_id": build_run_id("task_artifact"),
        "pipeline_run_id": pipeline_run_id,
        "task_run_id": task_run_id,
        "dataset_name": batch.dataset_name,
        "batch_id": batch.batch_id,
        "artifact_type": artifact_type,
        "direction": direction,
        "local_path": local_path,
        "gcs_uri": gcs_uri,
        "load_batch_id": load_batch_id,
        "source_file": source_file,
        "partition_key": partition_key,
        "checksum": checksum,
        "record_count": record_count,
        "payload_json": json_ready(payload_json or {}),
    }


def _collect_comtrade_lineage_artifacts(
    *,
    pipeline_run_id: str,
    task_run_id: str,
    batch: BatchDefinition,
    manifest_entry: dict[str, Any],
    cloud_config: GcpCloudConfig | None,
) -> list[dict[str, Any]]:
    audit_dir = _resolve_project_path(manifest_entry.get("audit_dir"))
    if audit_dir is None:
        return []

    artifacts: list[dict[str, Any]] = []
    bronze_inventory_path = audit_dir / "bronze_file_inventory.parquet"
    if bronze_inventory_path.exists():
        inventory = pd.read_parquet(bronze_inventory_path)
        for row in inventory.to_dict(orient="records"):
            source_path = row.get("source_path") or row.get("source_file")
            gcs_uri = None
            if cloud_config and row.get("source_year_partition") and row.get("source_file"):
                gcs_uri = cloud_config.gcs_uri(
                    "bronze",
                    "comtrade",
                    "monthly_history",
                    f"year={int(row['source_year_partition']):04d}",
                    row["source_file"],
                )
            artifacts.append(
                _task_artifact_row(
                    pipeline_run_id=pipeline_run_id,
                    task_run_id=task_run_id,
                    batch=batch,
                    artifact_type="bronze_source_file",
                    direction="input",
                    local_path=str(source_path) if source_path else None,
                    gcs_uri=gcs_uri,
                    load_batch_id=row.get("load_batch_id"),
                    source_file=row.get("source_file"),
                    partition_key=str(row.get("source_year_partition")) if row.get("source_year_partition") else None,
                    record_count=int(row["record_count"]) if row.get("record_count") is not None else None,
                    payload_json=row,
                )
            )

    fact_results_path = audit_dir / "fact_slice_results.parquet"
    if fact_results_path.exists():
        fact_results = pd.read_parquet(fact_results_path)
        for row in fact_results.to_dict(orient="records"):
            artifacts.append(
                _task_artifact_row(
                    pipeline_run_id=pipeline_run_id,
                    task_run_id=task_run_id,
                    batch=batch,
                    artifact_type="fact_slice_output",
                    direction="output",
                    local_path=row.get("path"),
                    partition_key=_detect_partition_key(str(row.get("path"))),
                    checksum=row.get("fingerprint"),
                    record_count=int(row["row_count"]) if row.get("row_count") is not None else None,
                    payload_json=row,
                )
            )
    return artifacts


def _collect_generic_artifacts(
    *,
    pipeline_run_id: str,
    task_run_id: str,
    batch: BatchDefinition,
    manifest_path: Path | None,
    task_log_path: Path,
    manifest_entry: dict[str, Any] | None,
) -> list[dict[str, Any]]:
    artifacts = [
        _task_artifact_row(
            pipeline_run_id=pipeline_run_id,
            task_run_id=task_run_id,
            batch=batch,
            artifact_type="task_log",
            direction="output",
            local_path=str(task_log_path),
            payload_json={"task_log_path": str(task_log_path)},
        )
    ]
    if manifest_path is not None:
        artifacts.append(
            _task_artifact_row(
                pipeline_run_id=pipeline_run_id,
                task_run_id=task_run_id,
                batch=batch,
                artifact_type="task_manifest",
                direction="output",
                local_path=str(manifest_path),
                payload_json={"manifest_path": str(manifest_path)},
            )
        )

    if not manifest_entry:
        return artifacts

    dimension_results = manifest_entry.get("dimension_results") or {}
    for dimension_name, result in dimension_results.items():
        artifacts.append(
            _task_artifact_row(
                pipeline_run_id=pipeline_run_id,
                task_run_id=task_run_id,
                batch=batch,
                artifact_type="dimension_output",
                direction="output",
                local_path=result.get("path"),
                partition_key=dimension_name,
                checksum=result.get("fingerprint"),
                record_count=int(result["row_count"]) if result.get("row_count") is not None else None,
                payload_json={"dimension_name": dimension_name, **result},
            )
        )

    uploads = manifest_entry.get("uploads") or {}
    for upload_name, upload_summary in uploads.items():
        artifacts.append(
            _task_artifact_row(
                pipeline_run_id=pipeline_run_id,
                task_run_id=task_run_id,
                batch=batch,
                artifact_type="gcs_publish_summary",
                direction="output",
                local_path=upload_summary.get("local_path"),
                gcs_uri=upload_summary.get("gcs_destination"),
                partition_key=upload_name,
                record_count=int(upload_summary["files_uploaded"]) if upload_summary.get("files_uploaded") is not None else None,
                payload_json={"upload_name": upload_name, **upload_summary},
            )
        )

    for key, artifact_type in (
        ("partitions_written", "local_partition_output"),
        ("daily_partitions_written", "daily_partition_output"),
        ("monthly_partitions_written", "monthly_partition_output"),
    ):
        for local_path in manifest_entry.get(key, []) or []:
            artifacts.append(
                _task_artifact_row(
                    pipeline_run_id=pipeline_run_id,
                    task_run_id=task_run_id,
                    batch=batch,
                    artifact_type=artifact_type,
                    direction="output",
                    local_path=local_path,
                    partition_key=_detect_partition_key(local_path),
                    payload_json={"source_key": key},
                )
            )

    for table_key in (
        "fact_table_id",
        "daily_table_id",
        "monthly_table_id",
        "state_table_id",
        "audit_table_id",
    ):
        if manifest_entry.get(table_key):
            artifacts.append(
                _task_artifact_row(
                    pipeline_run_id=pipeline_run_id,
                    task_run_id=task_run_id,
                    batch=batch,
                    artifact_type="bigquery_table",
                    direction="output",
                    partition_key=table_key,
                    payload_json={table_key: manifest_entry[table_key]},
                )
            )
    return artifacts


def _collect_task_artifacts(
    *,
    pipeline_run_id: str,
    task_run_id: str,
    batch: BatchDefinition,
    step: BatchStep,
    manifest_path: Path | None,
    task_log_path: Path,
    manifest_entry: dict[str, Any] | None,
    cloud_config: GcpCloudConfig | None,
) -> list[dict[str, Any]]:
    artifacts = _collect_generic_artifacts(
        pipeline_run_id=pipeline_run_id,
        task_run_id=task_run_id,
        batch=batch,
        manifest_path=manifest_path,
        task_log_path=task_log_path,
        manifest_entry=manifest_entry,
    )
    if batch.dataset_name == "comtrade" and step.task_name == "silver" and manifest_entry:
        artifacts.extend(
            _collect_comtrade_lineage_artifacts(
                pipeline_run_id=pipeline_run_id,
                task_run_id=task_run_id,
                batch=batch,
                manifest_entry=manifest_entry,
                cloud_config=cloud_config,
            )
        )
    return artifacts


def _checkpoint_row(
    *,
    pipeline_run_id: str,
    batch: BatchDefinition,
    partition_type: str,
    partition_key: str,
    last_task_name: str,
    status: str,
    checkpoint_value: str | None = None,
    retryable: bool | None = None,
    error_summary: str | None = None,
    metrics_json: dict[str, Any] | None = None,
) -> dict[str, Any]:
    return {
        "checkpoint_key": f"{batch.batch_id}|{partition_type}|{partition_key}",
        "pipeline_run_id": pipeline_run_id,
        "dataset_name": batch.dataset_name,
        "batch_id": batch.batch_id,
        "partition_type": partition_type,
        "partition_key": partition_key,
        "last_task_name": last_task_name,
        "status": status,
        "checkpoint_value": checkpoint_value,
        "retryable": retryable,
        "error_summary": error_summary,
        "metrics_json": metrics_json or {},
    }


def _build_partition_checkpoints(
    *,
    pipeline_run_id: str,
    batch: BatchDefinition,
    task_name: str,
    status: str,
    manifest_entry: dict[str, Any] | None,
    error_summary: str | None,
) -> list[dict[str, Any]]:
    checkpoints = [
        _checkpoint_row(
            pipeline_run_id=pipeline_run_id,
            batch=batch,
            partition_type="batch",
            partition_key=batch.batch_id,
            last_task_name=task_name,
            status=status,
            checkpoint_value=task_name,
            retryable=status != "completed",
            error_summary=error_summary,
            metrics_json={"task_name": task_name},
        )
    ]
    if not manifest_entry:
        return checkpoints

    key_groups = (
        ("year_month", manifest_entry.get("touched_year_months") or manifest_entry.get("selected_year_months") or manifest_entry.get("requested_year_months") or []),
        ("period", manifest_entry.get("requested_periods") or []),
    )
    for partition_type, values in key_groups:
        for value in values:
            checkpoints.append(
                _checkpoint_row(
                    pipeline_run_id=pipeline_run_id,
                    batch=batch,
                    partition_type=partition_type,
                    partition_key=str(value),
                    last_task_name=task_name,
                    status=status,
                    checkpoint_value=str(value),
                    retryable=status != "completed",
                    error_summary=error_summary,
                    metrics_json={"source_manifest_status": manifest_entry.get("status")},
                )
            )
    return checkpoints


@dataclass
class StepExecutionError(RuntimeError):
    task_name: str
    task_run_id: str
    failure_type: str
    error_summary: str

    def __str__(self) -> str:
        return self.error_summary


def _record_bigquery_snapshot(append_call, logger, description: str) -> None:
    try:
        append_call()
    except Exception as exc:
        logger.warning("Failed to mirror %s to BigQuery ops tables: %s", description, exc)
        if _strict_bigquery_mirror():
            raise


def _run_step_command(
    *,
    command: list[str],
    env: dict[str, str],
    task_log_path: Path,
) -> None:
    task_log_path.parent.mkdir(parents=True, exist_ok=True)
    with task_log_path.open("a", encoding="utf-8") as handle:
        handle.write(f"$ {' '.join(command)}\n")
        handle.flush()
        subprocess.run(
            command,
            cwd=PROJECT_ROOT,
            env=env,
            stdout=handle,
            stderr=subprocess.STDOUT,
            text=True,
            check=True,
        )


def _inject_preferred_python_path(env: dict[str, str]) -> dict[str, str]:
    candidates: list[Path] = []
    if env.get("VIRTUAL_ENV"):
        candidates.append(Path(env["VIRTUAL_ENV"]) / "bin")
    candidates.append(PROJECT_ROOT / ".venv" / "bin")
    candidates.append(Path("/workspace/.venv/bin"))

    for candidate in candidates:
        if not candidate.exists():
            continue

        current_path = env.get("PATH", "")
        path_entries = [entry for entry in current_path.split(":") if entry]
        candidate_str = str(candidate)
        if candidate_str in path_entries:
            return env

        env["PATH"] = f"{candidate_str}:{current_path}" if current_path else candidate_str
        return env

    return env


def _cleanup_local_paths(paths: list[Path], logger) -> list[str]:
    cleaned: list[str] = []
    for path in paths:
        try:
            path.relative_to(PROJECT_ROOT)
        except ValueError as exc:
            raise ValueError(f"Refusing to clean path outside project root: {path}") from exc
        if not path.exists():
            continue
        if path.is_dir():
            shutil.rmtree(path)
        else:
            path.unlink()
        cleaned.append(str(path))
        logger.info("Removed local path after successful batch completion: %s", path)
    return cleaned


def execute_batch(
    *,
    batch: BatchDefinition,
    plan_path: str | None = None,
    trigger_type: str = "manual",
    bruin_pipeline_name: str | None = None,
    attempt_number: int = 1,
) -> dict[str, Any]:
    store = PostgresOpsStore.from_env()
    store.ensure_schema()

    mirror: BigQueryOpsMirror | None = None
    if bigquery_mirror_enabled():
        try:
            mirror = BigQueryOpsMirror.from_env()
            mirror.ensure_tables()
        except Exception as exc:
            mirror = None
            if _strict_bigquery_mirror():
                raise
            print(f"Warning: BigQuery ops mirror unavailable: {exc}", file=sys.stderr)

    pipeline_run_id = build_run_id("pipeline_run")
    started_at = _utc_now()
    pipeline_log_path = _default_pipeline_log_path(batch, pipeline_run_id, attempt_number)
    logger = configure_logger(
        logger_name=f"ops.batch.{batch.dataset_name}.{batch.batch_id}",
        log_path=pipeline_log_path,
        log_level=os.getenv("OPS_LOG_LEVEL", "INFO"),
    )
    cloud_config = _safe_cloud_config()

    pipeline_run_row = {
        "pipeline_run_id": pipeline_run_id,
        "dataset_name": batch.dataset_name,
        "batch_id": batch.batch_id,
        "phase": batch.phase,
        "schedule_lane": batch.schedule_lane,
        "bruin_pipeline_name": bruin_pipeline_name,
        "trigger_type": trigger_type,
        "attempt_number": attempt_number,
        "max_attempts": batch.max_attempts,
        "planned_partition_count": batch.planned_partition_count,
        "planned_reporter_count": batch.planned_reporter_count,
        "planned_cmd_code_count": batch.planned_cmd_code_count,
        "planned_window_start": batch.planned_window_start,
        "planned_window_end": batch.planned_window_end,
        "status": "running",
        "started_at": started_at,
        "finished_at": None,
        "queue_drained": None,
        "should_retry": None,
        "next_retry_at": None,
        "retry_backoff_seconds": batch.retry_backoff_seconds,
        "log_path": str(pipeline_log_path),
        "gcs_log_uri": None,
        "error_summary": None,
        "run_args_json": {
            "plan_path": str(resolve_batch_plan_path(plan_path)),
            "batch_metadata": batch.batch_metadata,
            "depends_on_batch_ids": list(batch.depends_on_batch_ids),
            "cleanup_local_on_success": batch.cleanup_local_on_success,
        },
        "metrics_json": {
            "description": batch.description,
            "step_count": len(batch.steps),
        },
    }
    store.insert_pipeline_run(pipeline_run_row)
    if mirror is not None:
        _record_bigquery_snapshot(
            lambda: mirror.append_pipeline_run(pipeline_run_row),
            logger,
            "pipeline_run start",
        )

    logger.info(
        "Starting batch execution dataset=%s batch_id=%s attempt=%s pipeline_run_id=%s",
        batch.dataset_name,
        batch.batch_id,
        attempt_number,
        pipeline_run_id,
    )

    completed_tasks: list[str] = []
    task_summaries: list[dict[str, Any]] = []
    try:
        for step_order, step in enumerate(batch.steps, start=1):
            task_run_id = build_run_id("task_run")
            task_started = _utc_now()
            manifest_path = step.resolved_manifest_path()
            task_log_path = step.resolved_log_path() or _default_task_log_path(
                batch,
                step.task_name,
                step_order,
                attempt_number,
            )
            manifest_size_before = _manifest_size(manifest_path)
            task_status = "completed"
            error_summary = None
            failure_type = None

            logger.info("Running step %s/%s task=%s", step_order, len(batch.steps), step.task_name)
            command = _normalize_command(step.command)
            env = os.environ.copy()
            env.update(
                {
                    "PIPELINE_RUN_ID": pipeline_run_id,
                    "TASK_RUN_ID": task_run_id,
                    "BATCH_ID": batch.batch_id,
                    "DATASET_NAME": batch.dataset_name,
                    "TASK_NAME": step.task_name,
                    "BATCH_ATTEMPT_NUMBER": str(attempt_number),
                }
            )
            env = _inject_preferred_python_path(env)

            try:
                _run_step_command(
                    command=command,
                    env=env,
                    task_log_path=task_log_path,
                )
            except subprocess.CalledProcessError as exc:
                task_status = "failed"
                failure_type = exc.__class__.__name__
                error_summary = f"Command exited with status {exc.returncode}"
            except Exception as exc:
                task_status = "failed"
                failure_type = exc.__class__.__name__
                error_summary = str(exc)

            manifest_entries = _read_manifest_entries_since(manifest_path, manifest_size_before)
            manifest_entry = manifest_entries[-1] if manifest_entries else None
            if manifest_entry and task_status == "completed":
                task_status = str(manifest_entry.get("status", task_status))
                if task_status in {"loaded", "planned"}:
                    task_status = "completed"
            if manifest_entry and error_summary is None and manifest_entry.get("error_summary"):
                error_summary = str(manifest_entry["error_summary"])
                if task_status != "completed":
                    failure_type = failure_type or "ManifestReportedFailure"

            finished_at = _utc_now()
            task_run_row = {
                "task_run_id": task_run_id,
                "pipeline_run_id": pipeline_run_id,
                "dataset_name": batch.dataset_name,
                "batch_id": batch.batch_id,
                "task_name": step.task_name,
                "step_order": step_order,
                "status": task_status,
                "attempt_number": attempt_number,
                "started_at": task_started,
                "finished_at": finished_at,
                "duration_seconds": duration_seconds(task_started, finished_at),
                "local_manifest_path": str(manifest_path) if manifest_path else None,
                "gcs_manifest_uri": None,
                "log_path": str(task_log_path),
                "error_summary": error_summary,
                "command_json": command,
                "metrics_json": _task_metrics(step, manifest_entry),
            }
            store.insert_task_run(task_run_row)
            if mirror is not None:
                _record_bigquery_snapshot(
                    lambda row=task_run_row: mirror.append_task_run(row),
                    logger,
                    f"task_run {step.task_name}",
                )

            artifacts = _collect_task_artifacts(
                pipeline_run_id=pipeline_run_id,
                task_run_id=task_run_id,
                batch=batch,
                step=step,
                manifest_path=manifest_path,
                task_log_path=task_log_path,
                manifest_entry=manifest_entry,
                cloud_config=cloud_config,
            )
            store.insert_task_artifacts(artifacts)
            if mirror is not None and artifacts:
                _record_bigquery_snapshot(
                    lambda rows=artifacts: mirror.append_task_artifacts(rows),
                    logger,
                    f"task_artifacts {step.task_name}",
                )

            checkpoints = _build_partition_checkpoints(
                pipeline_run_id=pipeline_run_id,
                batch=batch,
                task_name=step.task_name,
                status=task_status,
                manifest_entry=manifest_entry,
                error_summary=error_summary,
            )
            store.upsert_partition_checkpoints(checkpoints)
            if mirror is not None:
                _record_bigquery_snapshot(
                    lambda rows=checkpoints: mirror.append_partition_checkpoints(rows),
                    logger,
                    f"partition checkpoints {step.task_name}",
                )

            task_summaries.append(
                {
                    "task_name": step.task_name,
                    "status": task_status,
                    "task_run_id": task_run_id,
                    "log_path": str(task_log_path),
                    "manifest_path": str(manifest_path) if manifest_path else None,
                }
            )
            if task_status != "completed":
                raise StepExecutionError(
                    task_name=step.task_name,
                    task_run_id=task_run_id,
                    failure_type=failure_type or "TaskExecutionError",
                    error_summary=error_summary or f"Task {step.task_name} failed",
                )
            completed_tasks.append(step.task_name)

        cleaned_paths: list[str] = []
        if batch.cleanup_local_on_success and batch.cleanup_paths:
            cleanup_task_run_id = build_run_id("task_run")
            cleanup_started = _utc_now()
            cleaned_paths = _cleanup_local_paths(batch.resolved_cleanup_paths(), logger)
            cleanup_finished = _utc_now()
            cleanup_row = {
                "task_run_id": cleanup_task_run_id,
                "pipeline_run_id": pipeline_run_id,
                "dataset_name": batch.dataset_name,
                "batch_id": batch.batch_id,
                "task_name": "cleanup_local",
                "step_order": len(batch.steps) + 1,
                "status": "completed",
                "attempt_number": attempt_number,
                "started_at": cleanup_started,
                "finished_at": cleanup_finished,
                "duration_seconds": duration_seconds(cleanup_started, cleanup_finished),
                "local_manifest_path": None,
                "gcs_manifest_uri": None,
                "log_path": str(pipeline_log_path),
                "error_summary": None,
                "command_json": ["cleanup_local"],
                "metrics_json": {"cleaned_paths": cleaned_paths},
            }
            store.insert_task_run(cleanup_row)
            if mirror is not None:
                _record_bigquery_snapshot(
                    lambda row=cleanup_row: mirror.append_task_run(row),
                    logger,
                    "cleanup task_run",
                )

        finished_at = _utc_now()
        pipeline_run_row.update(
            {
                "status": "completed",
                "finished_at": finished_at,
                "queue_drained": None,
                "should_retry": False,
                "next_retry_at": None,
                "error_summary": None,
                "metrics_json": {
                    "description": batch.description,
                    "completed_tasks": completed_tasks,
                    "task_summaries": task_summaries,
                    "cleaned_paths": cleaned_paths,
                },
            }
        )
        store.insert_pipeline_run(pipeline_run_row)
        if mirror is not None:
            _record_bigquery_snapshot(
                lambda: mirror.append_pipeline_run(pipeline_run_row),
                logger,
                "pipeline_run completion",
            )
        logger.info("Completed batch dataset=%s batch_id=%s", batch.dataset_name, batch.batch_id)
        return json_ready(pipeline_run_row)
    except StepExecutionError as exc:
        finished_at = _utc_now()
        next_retry_at = (
            finished_at.replace(microsecond=0)
            if attempt_number >= batch.max_attempts
            else finished_at + timedelta(seconds=batch.retry_backoff_seconds)
        )
        retryable = attempt_number < batch.max_attempts
        pipeline_run_row.update(
            {
                "status": "failed",
                "finished_at": finished_at,
                "should_retry": retryable,
                "next_retry_at": next_retry_at if retryable else None,
                "error_summary": exc.error_summary,
                "metrics_json": {
                    "description": batch.description,
                    "completed_tasks": completed_tasks,
                    "failed_task": exc.task_name,
                    "task_summaries": task_summaries,
                },
            }
        )
        store.insert_pipeline_run(pipeline_run_row)
        retry_row = {
            "retry_id": build_run_id("retry"),
            "pipeline_run_id": pipeline_run_id,
            "task_run_id": exc.task_run_id,
            "dataset_name": batch.dataset_name,
            "batch_id": batch.batch_id,
            "task_name": exc.task_name,
            "attempt_number": attempt_number,
            "max_attempts": batch.max_attempts,
            "status": "scheduled" if retryable else "exhausted",
            "failure_type": exc.failure_type,
            "http_status": None,
            "retryable": retryable,
            "next_retry_at": next_retry_at if retryable else None,
            "resolved_at": None,
            "error_summary": exc.error_summary,
            "payload_json": {
                "retry_backoff_seconds": batch.retry_backoff_seconds,
                "plan_path": str(resolve_batch_plan_path(plan_path)),
            },
        }
        store.upsert_retry_registry(retry_row)
        if mirror is not None:
            _record_bigquery_snapshot(
                lambda: mirror.append_pipeline_run(pipeline_run_row),
                logger,
                "pipeline_run failure",
            )
            _record_bigquery_snapshot(
                lambda: mirror.append_retry_registry(retry_row),
                logger,
                "retry_registry failure",
            )
        logger.error(
            "Batch failed dataset=%s batch_id=%s task=%s attempt=%s error=%s",
            batch.dataset_name,
            batch.batch_id,
            exc.task_name,
            attempt_number,
            exc.error_summary,
        )
        raise


def _build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run one manifest-defined dataset batch end to end.")
    parser.add_argument("dataset_name", help="Dataset name from ops/batch_plan.json")
    parser.add_argument("batch_id", help="Batch identifier from ops/batch_plan.json")
    parser.add_argument("--plan-path", default=None, help="Optional path to a batch-plan JSON file.")
    parser.add_argument("--trigger-type", default="manual", help="Trigger label stored in ops.pipeline_run.")
    parser.add_argument("--bruin-pipeline-name", default=None, help="Optional Bruin pipeline name for lineage.")
    parser.add_argument("--attempt-number", type=int, default=1, help="Attempt number for retry-aware queue runs.")
    return parser


def main() -> None:
    args = _build_arg_parser().parse_args()
    plan = load_batch_plan(args.plan_path)
    batch = plan.get(args.batch_id)
    if batch is None:
        raise SystemExit(f"Batch {args.batch_id} not found in {resolve_batch_plan_path(args.plan_path)}")
    if batch.dataset_name != args.dataset_name:
        raise SystemExit(
            f"Batch {args.batch_id} belongs to dataset {batch.dataset_name}, not {args.dataset_name}"
        )
    result = execute_batch(
        batch=batch,
        plan_path=args.plan_path,
        trigger_type=args.trigger_type,
        bruin_pipeline_name=args.bruin_pipeline_name,
        attempt_number=args.attempt_number,
    )
    print(json.dumps(result, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()
