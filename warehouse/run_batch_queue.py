from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from warehouse.batch_plan import batches_for_schedule_lane, load_batch_plan, resolve_batch_plan_path
from warehouse.ops_store import PostgresOpsStore
from warehouse.run_dataset_batch import execute_batch


def _latest_completed_batches() -> set[str]:
    store = PostgresOpsStore.from_env()
    store.ensure_schema()
    statuses = store.fetch_latest_batch_statuses()
    return {batch_id for batch_id, row in statuses.items() if row.get("status") == "completed"}


def _queue_summary_row(batch_id: str, status: str, payload: dict[str, Any] | None = None) -> dict[str, Any]:
    return {"batch_id": batch_id, "status": status, "payload": payload or {}}


def _build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run all eligible manifest-defined batches for one schedule lane.")
    parser.add_argument("schedule_lane", help="Schedule lane from ops/batch_plan.json")
    parser.add_argument("--plan-path", default=None, help="Optional batch-plan JSON path.")
    parser.add_argument("--trigger-type", default="scheduler", help="Trigger type recorded in pipeline_run.")
    parser.add_argument("--bruin-pipeline-name", default=None, help="Optional Bruin pipeline lineage label.")
    parser.add_argument("--max-batches", type=int, default=None, help="Optional cap on batches executed in this invocation.")
    return parser


def main() -> None:
    args = _build_arg_parser().parse_args()
    plan = load_batch_plan(args.plan_path)
    schedule_batches = batches_for_schedule_lane(plan, args.schedule_lane)
    if not schedule_batches:
        raise SystemExit(f"No enabled batches found for schedule lane {args.schedule_lane} in {resolve_batch_plan_path(args.plan_path)}")

    completed_batches = _latest_completed_batches()
    executed_count = 0
    results: list[dict[str, Any]] = []

    for batch in schedule_batches:
        if args.max_batches is not None and executed_count >= args.max_batches:
            break
        if batch.batch_id in completed_batches:
            results.append(_queue_summary_row(batch.batch_id, "skipped_completed"))
            continue
        missing_dependencies = [dependency for dependency in batch.depends_on_batch_ids if dependency not in completed_batches]
        if missing_dependencies:
            results.append(
                _queue_summary_row(
                    batch.batch_id,
                    "blocked_dependency",
                    {"missing_dependencies": missing_dependencies},
                )
            )
            continue

        attempt_result: dict[str, Any] | None = None
        for attempt_number in range(1, batch.max_attempts + 1):
            try:
                attempt_result = execute_batch(
                    batch=batch,
                    plan_path=args.plan_path,
                    trigger_type=args.trigger_type,
                    bruin_pipeline_name=args.bruin_pipeline_name,
                    attempt_number=attempt_number,
                )
                results.append(_queue_summary_row(batch.batch_id, "completed", attempt_result))
                completed_batches.add(batch.batch_id)
                executed_count += 1
                break
            except Exception as exc:
                if attempt_number >= batch.max_attempts:
                    results.append(
                        _queue_summary_row(
                            batch.batch_id,
                            "failed_exhausted",
                            {
                                "attempt_number": attempt_number,
                                "error_summary": str(exc),
                            },
                        )
                    )
                    executed_count += 1
                    break
                time.sleep(batch.retry_backoff_seconds)
        if attempt_result is None and batch.batch_id not in completed_batches:
            continue

    drained = True
    for batch in schedule_batches:
        if batch.batch_id in completed_batches:
            continue
        if all(dependency in completed_batches for dependency in batch.depends_on_batch_ids):
            drained = False
            break

    payload = {
        "plan_path": str(resolve_batch_plan_path(args.plan_path)),
        "schedule_lane": args.schedule_lane,
        "queue_drained": drained,
        "executed_batches": executed_count,
        "results": results,
    }
    print(json.dumps(payload, indent=2, ensure_ascii=False))
    if any(item["status"] == "failed_exhausted" for item in results):
        raise SystemExit(2)


if __name__ == "__main__":
    main()
