from __future__ import annotations

import argparse
import json
from datetime import date
from pathlib import Path

from google.api_core.exceptions import NotFound
from google.cloud import bigquery

from ingest.common.cloud_config import GcpCloudConfig


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_LOCAL_SILVER_ROOT = PROJECT_ROOT / "data" / "silver" / "portwatch" / "portwatch_monthly"
PARTITION_FILENAME = "portwatch_monthly.parquet"


def _partition_files(local_silver_root: Path) -> list[Path]:
    return sorted(local_silver_root.glob(f"year=*/month=*/{PARTITION_FILENAME}"))


def _month_start_from_path(path: Path) -> date:
    year = None
    month = None
    for part in path.parts:
        if part.startswith("year="):
            year = int(part.split("=", 1)[1])
        elif part.startswith("month="):
            month = int(part.split("=", 1)[1])
    if year is None or month is None:
        raise ValueError(f"Could not infer year/month partition from {path}")
    return date(year, month, 1)


def _gcs_uris_for_files(files: list[Path], local_silver_root: Path, config: GcpCloudConfig) -> list[str]:
    uris: list[str] = []
    for path in files:
        relative_path = path.relative_to(local_silver_root).as_posix()
        uris.append(config.gcs_uri("silver", "portwatch", "portwatch_monthly", relative_path))
    return uris


def _ensure_dataset(
    client: bigquery.Client,
    *,
    project_id: str,
    dataset_name: str,
    location: str,
) -> None:
    dataset = bigquery.Dataset(f"{project_id}.{dataset_name}")
    dataset.location = location
    client.create_dataset(dataset, exists_ok=True)


def load_portwatch_monthly(
    *,
    local_silver_root: Path,
    table_name: str,
    replace_touched_partitions: bool,
    dry_run: bool,
) -> dict[str, object]:
    config = GcpCloudConfig.from_env()
    partition_files = _partition_files(local_silver_root)
    if not partition_files:
        raise FileNotFoundError(
            f"No PortWatch partition files found under {local_silver_root}. "
            "Run the silver build before loading BigQuery."
        )

    touched_month_starts = sorted({_month_start_from_path(path) for path in partition_files})
    gcs_uris = _gcs_uris_for_files(partition_files, local_silver_root, config)
    table_id = f"{config.gcp_project_id}.{config.bq_raw_dataset}.{table_name}"

    summary: dict[str, object] = {
        "project_id": config.gcp_project_id,
        "location": config.gcp_location,
        "table_id": table_id,
        "local_silver_root": str(local_silver_root),
        "partition_file_count": len(partition_files),
        "touched_month_start_dates": [month.isoformat() for month in touched_month_starts],
        "gcs_source_uris": gcs_uris[:20],
        "replace_touched_partitions": replace_touched_partitions,
        "dry_run": dry_run,
    }

    if dry_run:
        summary["status"] = "planned"
        return summary

    client = bigquery.Client(project=config.gcp_project_id)
    _ensure_dataset(
        client,
        project_id=config.gcp_project_id,
        dataset_name=config.bq_raw_dataset,
        location=config.gcp_location,
    )

    table_exists = True
    try:
        client.get_table(table_id)
    except NotFound:
        table_exists = False

    if table_exists and replace_touched_partitions:
        delete_sql = f"""
        delete from `{table_id}`
        where month_start_date in unnest(@touched_month_start_dates)
        """
        delete_job = client.query(
            delete_sql,
            location=config.gcp_location,
            job_config=bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ArrayQueryParameter(
                        "touched_month_start_dates",
                        "DATE",
                        touched_month_starts,
                    )
                ]
            ),
        )
        delete_job.result()

    load_job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.PARQUET,
        create_disposition=bigquery.CreateDisposition.CREATE_IF_NEEDED,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        time_partitioning=bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY,
            field="month_start_date",
        ),
        clustering_fields=["chokepoint_id"],
    )
    load_job = client.load_table_from_uri(
        gcs_uris,
        table_id,
        location=config.gcp_location,
        job_config=load_job_config,
    )
    load_job.result()

    table = client.get_table(table_id)
    summary["status"] = "loaded"
    summary["output_rows"] = table.num_rows
    return summary


def _build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Load PortWatch monthly silver parquet from GCS into BigQuery raw.portwatch_monthly.")
    parser.add_argument(
        "--local-silver-root",
        default=str(DEFAULT_LOCAL_SILVER_ROOT),
        help="Local root for the canonical PortWatch silver monthly partitions.",
    )
    parser.add_argument("--table-name", default="portwatch_monthly", help="BigQuery table name inside the raw dataset.")
    parser.add_argument(
        "--append-only",
        action="store_true",
        help="Skip the delete step and append rows into the landing table.",
    )
    parser.add_argument("--dry-run", action="store_true", help="Show the intended load job without calling BigQuery.")
    return parser


def main() -> None:
    parser = _build_arg_parser()
    args = parser.parse_args()

    summary = load_portwatch_monthly(
        local_silver_root=Path(args.local_silver_root),
        table_name=args.table_name,
        replace_touched_partitions=not args.append_only,
        dry_run=args.dry_run,
    )
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
