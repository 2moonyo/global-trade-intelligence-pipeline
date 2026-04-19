from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from ingest.common.cloud_config import GcpCloudConfig
from ingest.common.gcs_io import download_file
from ingest.common.run_artifacts import json_ready
from warehouse.execution_profiles import current_runtime, runtime_for_dataset

DIM_COUNTRY_LOCAL_PATH = PROJECT_ROOT / "data" / "silver" / "comtrade" / "dimensions" / "dim_country.parquet"
EVENTS_SEED_PATH = PROJECT_ROOT / "data" / "seed" / "events" / "events_seed.csv"


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _bigquery_imports():
    try:
        from google.cloud import bigquery
    except ModuleNotFoundError as exc:
        raise ModuleNotFoundError(
            "google-cloud-bigquery is required for serverless World Bank preflight fallback hydration."
        ) from exc
    return bigquery


def _assert_runtime_owner(dataset_name: str) -> dict[str, Any]:
    runtime = current_runtime(default="cloud_run")
    owner_runtime = runtime_for_dataset(dataset_name)
    if owner_runtime != runtime:
        raise RuntimeError(
            f"Dataset {dataset_name!r} is owned by runtime {owner_runtime!r} in EXECUTION_PROFILE "
            f"{os.getenv('EXECUTION_PROFILE', 'all_vm')!r}, but this job is running as {runtime!r}."
        )
    return {"dataset_name": dataset_name, "execution_runtime": runtime, "owner_runtime": owner_runtime}


def _hydrate_dim_country_from_gcs(*, config: GcpCloudConfig, destination_path: Path) -> dict[str, Any]:
    blob_name = config.blob_path("silver", "comtrade", "dimensions", "dim_country.parquet")
    download_file(
        bucket_name=config.gcs_bucket,
        source_blob_name=blob_name,
        destination_path=destination_path,
        project_id=config.gcp_project_id,
    )
    return {
        "source": "gcs",
        "gcs_uri": config.gcs_uri("silver", "comtrade", "dimensions", "dim_country.parquet"),
        "local_path": str(destination_path),
    }


def _hydrate_dim_country_from_bigquery(*, config: GcpCloudConfig, destination_path: Path) -> dict[str, Any]:
    bigquery = _bigquery_imports()
    client = bigquery.Client(project=config.gcp_project_id)
    table_id = f"{config.gcp_project_id}.{config.bq_raw_dataset}.dim_country"
    query = f"select * from `{table_id}`"
    rows = [dict(row.items()) for row in client.query(query, location=config.gcp_location).result()]
    if not rows:
        raise RuntimeError(f"BigQuery fallback table {table_id} returned no rows for dim_country hydration.")
    destination_path.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(rows).to_parquet(destination_path, index=False)
    return {
        "source": "bigquery",
        "table_id": table_id,
        "local_path": str(destination_path),
        "row_count": len(rows),
    }


def ensure_worldbank_dim_country(*, force: bool) -> dict[str, Any]:
    if DIM_COUNTRY_LOCAL_PATH.exists() and not force:
        return {
            "status": "skipped_existing",
            "local_path": str(DIM_COUNTRY_LOCAL_PATH),
        }

    config = GcpCloudConfig.from_env()
    try:
        hydrated = _hydrate_dim_country_from_gcs(config=config, destination_path=DIM_COUNTRY_LOCAL_PATH)
        return {"status": "hydrated", **hydrated}
    except Exception as gcs_exc:
        hydrated = _hydrate_dim_country_from_bigquery(config=config, destination_path=DIM_COUNTRY_LOCAL_PATH)
        return {
            "status": "hydrated",
            **hydrated,
            "gcs_error_summary": str(gcs_exc),
        }


def ensure_events_seed() -> dict[str, Any]:
    if not EVENTS_SEED_PATH.exists():
        raise FileNotFoundError(
            f"Events seed file is missing from the runtime image: {EVENTS_SEED_PATH}. "
            "Check .dockerignore and docker/pipeline/Dockerfile seed inclusion."
        )
    return {"status": "present", "local_path": str(EVENTS_SEED_PATH)}


def run_preflight(*, dataset_name: str, batch_id: str | None, force: bool) -> dict[str, Any]:
    checks: dict[str, Any] = {
        "started_at": _utc_now_iso(),
        "dataset_name": dataset_name,
        "batch_id": batch_id,
        "runtime_owner": _assert_runtime_owner(dataset_name),
    }

    if dataset_name == "worldbank_energy":
        checks["worldbank_dim_country"] = ensure_worldbank_dim_country(force=force)
    elif dataset_name == "events":
        checks["events_seed"] = ensure_events_seed()
    else:
        checks["dataset_preflight"] = {"status": "no_dataset_specific_checks"}

    checks["finished_at"] = _utc_now_iso()
    checks["status"] = "completed"
    return json_ready(checks)


def _build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run Cloud Run preflight checks for serverless dataset batches.")
    parser.add_argument("--dataset-name", required=True, help="Dataset name from ops/batch_plan.json.")
    parser.add_argument("--batch-id", default=None, help="Optional batch id for diagnostics.")
    parser.add_argument("--force", action="store_true", help="Force rehydration of local dependency files.")
    return parser


def main() -> None:
    args = _build_arg_parser().parse_args()
    payload = run_preflight(dataset_name=args.dataset_name, batch_id=args.batch_id, force=args.force)
    print(json.dumps(payload, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()
