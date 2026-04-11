from __future__ import annotations

import json
import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_BATCH_PLAN_PATH = PROJECT_ROOT / "ops" / "batch_plan.json"


def _as_list(value: Any) -> list[Any]:
    if value is None:
        return []
    if isinstance(value, list):
        return value
    return [value]


@dataclass(frozen=True)
class BatchStep:
    task_name: str
    command: tuple[str, ...]
    manifest_path: str | None = None
    log_path: str | None = None
    notes: str | None = None

    def resolved_manifest_path(self) -> Path | None:
        if not self.manifest_path:
            return None
        return (PROJECT_ROOT / self.manifest_path).resolve()

    def resolved_log_path(self) -> Path | None:
        if not self.log_path:
            return None
        return (PROJECT_ROOT / self.log_path).resolve()


@dataclass(frozen=True)
class BatchDefinition:
    batch_id: str
    dataset_name: str
    description: str
    schedule_lane: str
    phase: str
    run_order: int = 0
    enabled: bool = True
    max_attempts: int = 3
    retry_backoff_seconds: int = 60
    cleanup_local_on_success: bool = False
    cleanup_paths: tuple[str, ...] = ()
    depends_on_batch_ids: tuple[str, ...] = ()
    dbt_selector: str | None = None
    planned_partition_count: int | None = None
    planned_reporter_count: int | None = None
    planned_cmd_code_count: int | None = None
    planned_window_start: str | None = None
    planned_window_end: str | None = None
    batch_metadata: dict[str, Any] = field(default_factory=dict)
    steps: tuple[BatchStep, ...] = ()

    def resolved_cleanup_paths(self) -> list[Path]:
        return [(PROJECT_ROOT / path).resolve() for path in self.cleanup_paths]


def resolve_batch_plan_path(path: str | None = None) -> Path:
    configured = path or os.getenv("BATCH_PLAN_PATH")
    return Path(configured).resolve() if configured else DEFAULT_BATCH_PLAN_PATH


def load_batch_plan(path: str | None = None) -> dict[str, BatchDefinition]:
    plan_path = resolve_batch_plan_path(path)
    payload = json.loads(plan_path.read_text(encoding="utf-8"))
    raw_batches = payload.get("batches")
    if not isinstance(raw_batches, list):
        raise ValueError(f"Expected 'batches' list in {plan_path}")

    definitions: dict[str, BatchDefinition] = {}
    for raw_batch in raw_batches:
        steps = tuple(
            BatchStep(
                task_name=str(step["task_name"]),
                command=tuple(str(part) for part in _as_list(step.get("command"))),
                manifest_path=step.get("manifest_path"),
                log_path=step.get("log_path"),
                notes=step.get("notes"),
            )
            for step in _as_list(raw_batch.get("steps"))
        )
        batch = BatchDefinition(
            batch_id=str(raw_batch["batch_id"]),
            dataset_name=str(raw_batch["dataset_name"]),
            description=str(raw_batch.get("description") or raw_batch["batch_id"]),
            schedule_lane=str(raw_batch["schedule_lane"]),
            phase=str(raw_batch.get("phase", "unspecified")),
            run_order=int(raw_batch.get("run_order", 0)),
            enabled=bool(raw_batch.get("enabled", True)),
            max_attempts=int(raw_batch.get("max_attempts", 3)),
            retry_backoff_seconds=int(raw_batch.get("retry_backoff_seconds", 60)),
            cleanup_local_on_success=bool(raw_batch.get("cleanup_local_on_success", False)),
            cleanup_paths=tuple(str(path) for path in _as_list(raw_batch.get("cleanup_paths"))),
            depends_on_batch_ids=tuple(str(batch_id) for batch_id in _as_list(raw_batch.get("depends_on_batch_ids"))),
            dbt_selector=raw_batch.get("dbt_selector"),
            planned_partition_count=raw_batch.get("planned_partition_count"),
            planned_reporter_count=raw_batch.get("planned_reporter_count"),
            planned_cmd_code_count=raw_batch.get("planned_cmd_code_count"),
            planned_window_start=raw_batch.get("planned_window_start"),
            planned_window_end=raw_batch.get("planned_window_end"),
            batch_metadata=dict(raw_batch.get("batch_metadata") or {}),
            steps=steps,
        )
        if batch.batch_id in definitions:
            raise ValueError(f"Duplicate batch_id in {plan_path}: {batch.batch_id}")
        if not batch.steps:
            raise ValueError(f"Batch {batch.batch_id} in {plan_path} has no steps")
        definitions[batch.batch_id] = batch
    return definitions


def batches_for_schedule_lane(plan: dict[str, BatchDefinition], schedule_lane: str) -> list[BatchDefinition]:
    return sorted(
        [batch for batch in plan.values() if batch.schedule_lane == schedule_lane and batch.enabled],
        key=lambda batch: (batch.run_order, batch.dataset_name, batch.batch_id),
    )
