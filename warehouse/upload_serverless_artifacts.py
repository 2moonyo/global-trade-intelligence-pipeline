from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from ingest.common.cloud_config import GcpCloudConfig
from ingest.common.gcs_io import upload_file
from ingest.common.run_artifacts import json_ready

DEFAULT_SUFFIXES = (".jsonl", ".json", ".log")


def _utc_stamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def _candidate_artifacts(dataset_name: str, batch_id: str, suffixes: tuple[str, ...]) -> list[Path]:
    roots = [
        PROJECT_ROOT / "logs" / dataset_name,
        PROJECT_ROOT / "logs" / "ops" / dataset_name / batch_id,
    ]
    files: list[Path] = []
    for root in roots:
        if not root.exists():
            continue
        files.extend(path for path in root.rglob("*") if path.is_file() and path.suffix in suffixes)
    for target_file in (PROJECT_ROOT / "target" / "run_results.json", PROJECT_ROOT / "target" / "manifest.json"):
        if target_file.exists():
            files.append(target_file)
    return sorted(set(files))


def upload_artifacts(
    *,
    dataset_name: str,
    batch_id: str,
    run_label: str,
    status: str,
    suffixes: tuple[str, ...] = DEFAULT_SUFFIXES,
    dry_run: bool = False,
) -> dict[str, Any]:
    config = GcpCloudConfig.from_env()
    files = _candidate_artifacts(dataset_name, batch_id, suffixes)
    destination_base = config.blob_path(
        "metadata",
        "serverless_runs",
        f"profile={os.getenv('EXECUTION_PROFILE', 'hybrid_vm_serverless')}",
        f"dataset={dataset_name}",
        f"batch_id={batch_id}",
        f"run={run_label}",
    )

    uploads: list[dict[str, Any]] = []
    for path in files:
        relative_path = path.relative_to(PROJECT_ROOT).as_posix()
        destination_blob_name = config.join_relative_parts(destination_base, relative_path)
        result = upload_file(
            path,
            bucket_name=config.gcs_bucket,
            destination_blob_name=destination_blob_name,
            project_id=config.gcp_project_id,
            skip_if_exists=False,
            dry_run=dry_run,
        )
        uploads.append(
            {
                "local_path": str(path),
                "gcs_uri": result.uri,
                "action": result.action,
            }
        )

    summary = {
        "status": "completed",
        "batch_status": status,
        "dataset_name": dataset_name,
        "batch_id": batch_id,
        "run_label": run_label,
        "destination_prefix": f"gs://{config.gcs_bucket}/{destination_base}",
        "artifact_count": len(files),
        "uploads": uploads,
        "dry_run": dry_run,
    }
    return json_ready(summary)


def _build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Best-effort upload of Cloud Run local logs/manifests to GCS metadata prefix.")
    parser.add_argument("--dataset-name", required=True)
    parser.add_argument("--batch-id", required=True)
    parser.add_argument("--run-label", default=None)
    parser.add_argument("--status", default="unknown")
    parser.add_argument("--dry-run", action="store_true")
    return parser


def main() -> None:
    args = _build_arg_parser().parse_args()
    run_label = args.run_label or os.getenv("SERVERLESS_RUN_ID") or f"serverless_{_utc_stamp()}"
    summary = upload_artifacts(
        dataset_name=args.dataset_name,
        batch_id=args.batch_id,
        run_label=run_label,
        status=args.status,
        dry_run=args.dry_run,
    )
    print(json.dumps(summary, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()
