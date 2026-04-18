from __future__ import annotations

import argparse
import json
import sys
from datetime import date, datetime, timezone
from pathlib import Path
from urllib.parse import urlparse

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from ingest.common.cloud_config import GcpCloudConfig
from ingest.common.gcs_io import list_blob_metadata, list_blob_uris
from ingest.common.run_artifacts import (
    append_manifest,
    build_run_id,
    configure_logger,
    duration_seconds,
    json_ready,
)
from warehouse.bigquery_load_state import (
    LoadStateRecord,
    blob_name_from_gcs_uri,
    ensure_load_state_table,
    fetch_load_state_checksums,
    replace_load_state_rows,
)


DEFAULT_LOCAL_MONTHLY_ROOT = PROJECT_ROOT / "data" / "silver" / "fx" / "ecb_fx_eu_monthly"
DEFAULT_GCS_PREFIX_PARTS = ("silver", "fx", "ecb_fx_eu_monthly")
PARTITION_FILENAME = "ecb_fx_eu_monthly.parquet"
LOGGER_NAME = "fx.load_bigquery"
LOG_DIR = PROJECT_ROOT / "logs" / "fx"
LOG_PATH = LOG_DIR / "load_fx_to_bigquery.log"
MANIFEST_PATH = LOG_DIR / "load_fx_to_bigquery_manifest.jsonl"
AUDIT_TABLE_NAME = "fx_load_audit"
STATE_TABLE_NAME = "fx_load_state"


def _bigquery_imports():
    try:
        from google.api_core.exceptions import NotFound
        from google.cloud import bigquery
    except ModuleNotFoundError as exc:
        raise ModuleNotFoundError(
            "google-cloud-bigquery is required for FX BigQuery load operations."
        ) from exc
    return bigquery, NotFound


def _parse_year_month(value: str) -> str:
    parsed = date.fromisoformat(f"{value}-01")
    return parsed.strftime("%Y-%m")


def _month_from_partition_parts(parts: tuple[str, ...]) -> date:
    year = None
    month = None
    for part in parts:
        if part.startswith("year="):
            year = int(part.split("=", 1)[1])
        elif part.startswith("month="):
            month = int(part.split("=", 1)[1])
    if year is None or month is None:
        raise ValueError(f"Could not infer year/month partition from {'/'.join(parts)}")
    return date(year, month, 1)


def _month_from_gcs_uri(uri: str) -> date:
    parsed = urlparse(uri)
    return _month_from_partition_parts(tuple(part for part in parsed.path.strip("/").split("/") if part))


def _partition_files(local_root: Path) -> list[Path]:
    return sorted(local_root.glob(f"year=*/month=*/{PARTITION_FILENAME}"))


def _gcs_partition_uris(config: GcpCloudConfig, gcs_prefix: str) -> list[str]:
    return list_blob_uris(
        bucket_name=config.gcs_bucket,
        prefix=gcs_prefix,
        project_id=config.gcp_project_id,
        suffix=PARTITION_FILENAME,
    )


def _uri_from_resolved_prefix(*, bucket_name: str, resolved_prefix: str, relative_path: str) -> str:
    return f"gs://{bucket_name}/{resolved_prefix.strip('/')}/{relative_path.lstrip('/')}"


def _matches_year_month_filters(
    *,
    year_month: str,
    selected_year_months: set[str],
    since_year_month: str | None,
    until_year_month: str | None,
) -> bool:
    if selected_year_months and year_month not in selected_year_months:
        return False
    if since_year_month and year_month < since_year_month:
        return False
    if until_year_month and year_month > until_year_month:
        return False
    return True


def _filter_candidate_uris(
    *,
    candidate_uris: list[str],
    selected_year_months: set[str],
    since_year_month: str | None,
    until_year_month: str | None,
) -> tuple[list[str], list[date]]:
    filtered: list[str] = []
    months: list[date] = []
    for uri in candidate_uris:
        month_start = _month_from_gcs_uri(uri)
        year_month = month_start.strftime("%Y-%m")
        if _matches_year_month_filters(
            year_month=year_month,
            selected_year_months=selected_year_months,
            since_year_month=since_year_month,
            until_year_month=until_year_month,
        ):
            filtered.append(uri)
            months.append(month_start)
    return filtered, sorted(set(months))


def _ensure_dataset(client, *, bigquery, project_id: str, dataset_name: str, location: str) -> None:
    dataset = bigquery.Dataset(f"{project_id}.{dataset_name}")
    dataset.location = location
    client.create_dataset(dataset, exists_ok=True)


def _ensure_audit_table(client, *, bigquery, project_id: str, dataset_name: str) -> str:
    table_id = f"{project_id}.{dataset_name}.{AUDIT_TABLE_NAME}"
    schema = [
        bigquery.SchemaField("run_id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("started_at", "TIMESTAMP"),
        bigquery.SchemaField("finished_at", "TIMESTAMP"),
        bigquery.SchemaField("status", "STRING"),
        bigquery.SchemaField("table_id", "STRING"),
        bigquery.SchemaField("source_mode", "STRING"),
        bigquery.SchemaField("gcs_prefix", "STRING"),
        bigquery.SchemaField("candidate_year_months", "STRING", mode="REPEATED"),
        bigquery.SchemaField("selected_year_months", "STRING", mode="REPEATED"),
        bigquery.SchemaField("skipped_loaded_year_months", "STRING", mode="REPEATED"),
        bigquery.SchemaField("candidate_file_count", "INT64"),
        bigquery.SchemaField("selected_file_count", "INT64"),
        bigquery.SchemaField("replace_touched_partitions", "BOOL"),
        bigquery.SchemaField("include_loaded_months", "BOOL"),
        bigquery.SchemaField("output_rows", "INT64"),
        bigquery.SchemaField("error_summary", "STRING"),
    ]
    table = bigquery.Table(table_id, schema=schema)
    client.create_table(table, exists_ok=True)
    return table_id


def _write_audit_row(client, *, audit_table_id: str, entry: dict[str, object]) -> list[dict[str, object]]:
    row = {
        "run_id": entry.get("run_id"),
        "started_at": entry.get("started_at"),
        "finished_at": entry.get("finished_at"),
        "status": entry.get("status"),
        "table_id": entry.get("table_id"),
        "source_mode": entry.get("source_mode"),
        "gcs_prefix": entry.get("gcs_prefix"),
        "candidate_year_months": entry.get("candidate_year_months", []),
        "selected_year_months": entry.get("selected_year_months", []),
        "skipped_loaded_year_months": entry.get("skipped_loaded_year_months", []),
        "candidate_file_count": entry.get("candidate_file_count"),
        "selected_file_count": entry.get("selected_file_count"),
        "replace_touched_partitions": entry.get("replace_touched_partitions"),
        "include_loaded_months": entry.get("include_loaded_months"),
        "output_rows": entry.get("output_rows"),
        "error_summary": entry.get("error_summary"),
    }
    return client.insert_rows_json(audit_table_id, [row])


def _source_state_by_month(
    *,
    config: GcpCloudConfig,
    resolved_gcs_prefix: str,
    candidate_uris: list[str],
    table_id: str,
) -> dict[str, LoadStateRecord]:
    metadata_by_name = list_blob_metadata(
        bucket_name=config.gcs_bucket,
        prefix=resolved_gcs_prefix,
        project_id=config.gcp_project_id,
        suffix=PARTITION_FILENAME,
    )
    month_state: dict[str, LoadStateRecord] = {}
    for uri in candidate_uris:
        blob_name = blob_name_from_gcs_uri(uri)
        blob_metadata = metadata_by_name.get(blob_name)
        if blob_metadata is None:
            raise FileNotFoundError(f"Expected GCS object for FX load source {uri}")
        if not blob_metadata.md5_hash:
            raise ValueError(f"GCS object {uri} is missing an md5 checksum; cannot perform checksum-aware load.")
        month_key = _month_from_gcs_uri(uri).strftime("%Y-%m")
        month_state[month_key] = LoadStateRecord(
            entity_type="monthly_partition",
            entity_key=month_key,
            source_checksum=blob_metadata.md5_hash,
            checksum_kind="gcs_md5",
            source_uris=(uri,),
            target_table_id=table_id,
        )
    return month_state


def _selected_months(
    *,
    month_state: dict[str, LoadStateRecord],
    include_loaded_months: bool,
    state_checksums: dict[str, str],
    table_exists: bool,
) -> tuple[list[str], list[str]]:
    candidate_months = sorted(month_state.keys())
    if include_loaded_months or not table_exists:
        return candidate_months, []

    selected: list[str] = []
    skipped: list[str] = []
    for month in candidate_months:
        prior_checksum = state_checksums.get(month)
        if prior_checksum is not None and prior_checksum == month_state[month].source_checksum:
            skipped.append(month)
        else:
            selected.append(month)
    return selected, skipped


def load_fx(
    *,
    source_mode: str,
    local_monthly_root: Path,
    monthly_gcs_prefix: str | None,
    monthly_table_name: str,
    replace_touched_partitions: bool,
    include_loaded_months: bool,
    selected_year_months: set[str],
    since_year_month: str | None,
    until_year_month: str | None,
    dry_run: bool,
    log_level: str,
) -> dict[str, object]:
    config = GcpCloudConfig.from_env()
    resolved_monthly_prefix = monthly_gcs_prefix or config.blob_path(*DEFAULT_GCS_PREFIX_PARTS)
    table_id = f"{config.gcp_project_id}.{config.bq_raw_dataset}.{monthly_table_name}"
    audit_table_id = f"{config.gcp_project_id}.{config.bq_raw_dataset}.{AUDIT_TABLE_NAME}"
    state_table_id = f"{config.gcp_project_id}.{config.bq_raw_dataset}.{STATE_TABLE_NAME}"
    run_id = build_run_id("fx_load_bigquery")
    logger = configure_logger(logger_name=LOGGER_NAME, log_path=LOG_PATH, log_level=log_level)
    started_at = datetime.now(timezone.utc)

    manifest_entry: dict[str, object] = {
        "run_id": run_id,
        "asset_name": "load_fx_to_bigquery",
        "dataset_name": "fx",
        "status": "running",
        "started_at": started_at.isoformat(),
        "finished_at": None,
        "duration_seconds": None,
        "table_id": table_id,
        "source_mode": source_mode,
        "gcs_prefix": resolved_monthly_prefix,
        "replace_touched_partitions": replace_touched_partitions,
        "include_loaded_months": include_loaded_months,
        "selected_year_months_filter": sorted(selected_year_months),
        "since_year_month": since_year_month,
        "until_year_month": until_year_month,
        "candidate_year_months": [],
        "selected_year_months": [],
        "skipped_loaded_year_months": [],
        "candidate_file_count": 0,
        "selected_file_count": 0,
        "output_rows": None,
        "audit_table_id": audit_table_id,
        "state_table_id": state_table_id,
        "error_summary": None,
        "dry_run": dry_run,
    }

    client = None
    audit_table_ready = False

    try:
        logger.info("Starting FX BigQuery load run_id=%s", run_id)
        logger.info("Step 1/4 Discover candidate FX monthly parquet partitions")

        if source_mode == "local":
            partition_files = _partition_files(local_monthly_root)
            if not partition_files:
                raise FileNotFoundError(f"No FX monthly partition files found under {local_monthly_root}")
            candidate_uris = [
                _uri_from_resolved_prefix(
                    bucket_name=config.gcs_bucket,
                    resolved_prefix=resolved_monthly_prefix,
                    relative_path=path.relative_to(local_monthly_root).as_posix(),
                )
                for path in partition_files
            ]
        else:
            candidate_uris = _gcs_partition_uris(config, resolved_monthly_prefix)
            if not candidate_uris:
                raise FileNotFoundError(
                    f"No FX monthly parquet objects found under gs://{config.gcs_bucket}/{resolved_monthly_prefix}"
                )

        filtered_candidate_uris, candidate_months = _filter_candidate_uris(
            candidate_uris=candidate_uris,
            selected_year_months=selected_year_months,
            since_year_month=since_year_month,
            until_year_month=until_year_month,
        )
        if not filtered_candidate_uris:
            raise FileNotFoundError("No FX monthly partition files matched the requested year-month filters.")

        candidate_year_months = [month.strftime("%Y-%m") for month in candidate_months]
        summary: dict[str, object] = {
            "run_id": run_id,
            "project_id": config.gcp_project_id,
            "location": config.gcp_location,
            "table_id": table_id,
            "source_mode": source_mode,
            "local_monthly_root": str(local_monthly_root),
            "gcs_prefix": resolved_monthly_prefix,
            "candidate_file_count": len(filtered_candidate_uris),
            "candidate_year_months": candidate_year_months,
            "replace_touched_partitions": replace_touched_partitions,
            "include_loaded_months": include_loaded_months,
            "selected_year_months_filter": sorted(selected_year_months),
            "since_year_month": since_year_month,
            "until_year_month": until_year_month,
            "dry_run": dry_run,
            "log_path": str(LOG_PATH),
            "manifest_path": str(MANIFEST_PATH),
            "audit_table_id": audit_table_id,
            "state_table_id": state_table_id,
        }
        manifest_entry["candidate_file_count"] = len(filtered_candidate_uris)
        manifest_entry["candidate_year_months"] = candidate_year_months

        if dry_run:
            finished_at = datetime.now(timezone.utc)
            summary["status"] = "planned"
            summary["gcs_source_uris"] = filtered_candidate_uris[:20]
            summary["duration_seconds"] = duration_seconds(started_at, finished_at)
            manifest_entry.update(
                {
                    "status": "planned",
                    "finished_at": finished_at.isoformat(),
                    "duration_seconds": duration_seconds(started_at, finished_at),
                }
            )
            append_manifest(MANIFEST_PATH, manifest_entry)
            return json_ready(summary)

        month_state = _source_state_by_month(
            config=config,
            resolved_gcs_prefix=resolved_monthly_prefix,
            candidate_uris=filtered_candidate_uris,
            table_id=table_id,
        )

        logger.info("Step 2/4 Resolve BigQuery dataset and selected months")
        bigquery, NotFound = _bigquery_imports()
        client = bigquery.Client(project=config.gcp_project_id)
        _ensure_dataset(
            client,
            bigquery=bigquery,
            project_id=config.gcp_project_id,
            dataset_name=config.bq_raw_dataset,
            location=config.gcp_location,
        )
        audit_table_id = _ensure_audit_table(
            client,
            bigquery=bigquery,
            project_id=config.gcp_project_id,
            dataset_name=config.bq_raw_dataset,
        )
        audit_table_ready = True
        ensure_load_state_table(
            client,
            bigquery=bigquery,
            project_id=config.gcp_project_id,
            dataset_name=config.bq_raw_dataset,
            table_name=STATE_TABLE_NAME,
        )

        table_exists = True
        try:
            client.get_table(table_id)
        except NotFound:
            table_exists = False

        state_checksums = fetch_load_state_checksums(
            client,
            bigquery=bigquery,
            state_table_id=state_table_id,
            entity_type="monthly_partition",
            entity_keys=sorted(month_state.keys()),
            location=config.gcp_location,
        )
        selected_months, skipped_months = _selected_months(
            month_state=month_state,
            include_loaded_months=include_loaded_months,
            state_checksums=state_checksums,
            table_exists=table_exists,
        )
        selected_uris = [month_state[month].source_uris[0] for month in selected_months]

        summary["selected_file_count"] = len(selected_uris)
        summary["selected_year_months"] = selected_months
        summary["skipped_loaded_year_months"] = skipped_months
        summary["gcs_source_uris"] = selected_uris[:20]
        manifest_entry["selected_file_count"] = len(selected_uris)
        manifest_entry["selected_year_months"] = selected_months
        manifest_entry["skipped_loaded_year_months"] = skipped_months

        if not selected_uris:
            finished_at = datetime.now(timezone.utc)
            summary["status"] = "no_op_all_candidate_months_already_loaded"
            summary["duration_seconds"] = duration_seconds(started_at, finished_at)
            manifest_entry.update(
                {
                    "status": summary["status"],
                    "finished_at": finished_at.isoformat(),
                    "duration_seconds": duration_seconds(started_at, finished_at),
                }
            )
            audit_errors = _write_audit_row(client, audit_table_id=audit_table_id, entry=manifest_entry)
            if audit_errors:
                logger.warning("Audit row insert returned errors: %s", audit_errors)
            append_manifest(MANIFEST_PATH, manifest_entry)
            return json_ready(summary)

        logger.info("Step 3/4 Submit FX monthly load job")
        if table_exists and replace_touched_partitions:
            delete_job = client.query(
                f"""
                delete from `{table_id}`
                where cast(month_start_date as date) in unnest(@touched_month_start_dates)
                """,
                location=config.gcp_location,
                job_config=bigquery.QueryJobConfig(
                    query_parameters=[
                        bigquery.ArrayQueryParameter(
                            "touched_month_start_dates",
                            "DATE",
                            [date.fromisoformat(f"{value}-01") for value in selected_months],
                        ),
                    ]
                ),
            )
            delete_job.result()

        load_job = client.load_table_from_uri(
            selected_uris,
            table_id,
            location=config.gcp_location,
            job_config=bigquery.LoadJobConfig(
                source_format=bigquery.SourceFormat.PARQUET,
                create_disposition=bigquery.CreateDisposition.CREATE_IF_NEEDED,
                write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
                schema_update_options=[bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION],
                time_partitioning=bigquery.TimePartitioning(
                    type_=bigquery.TimePartitioningType.DAY,
                    field="month_start_date",
                ),
                clustering_fields=["base_currency_code", "quote_currency_code"],
            ),
        )
        load_job.result()

        replace_load_state_rows(
            client,
            bigquery=bigquery,
            state_table_id=state_table_id,
            rows=[month_state[month] for month in selected_months],
            run_id=run_id,
            source_mode=source_mode,
            location=config.gcp_location,
        )

        logger.info("Step 4/4 Read final FX row count")
        output_rows = client.get_table(table_id).num_rows
        finished_at = datetime.now(timezone.utc)
        summary["status"] = "loaded"
        summary["output_rows"] = output_rows
        summary["duration_seconds"] = duration_seconds(started_at, finished_at)
        manifest_entry.update(
            {
                "status": "loaded",
                "finished_at": finished_at.isoformat(),
                "duration_seconds": duration_seconds(started_at, finished_at),
                "output_rows": output_rows,
            }
        )
        audit_errors = _write_audit_row(client, audit_table_id=audit_table_id, entry=manifest_entry)
        if audit_errors:
            logger.warning("Audit row insert returned errors: %s", audit_errors)
        append_manifest(MANIFEST_PATH, manifest_entry)
        return json_ready(summary)
    except Exception as exc:
        finished_at = datetime.now(timezone.utc)
        manifest_entry.update(
            {
                "status": "failed",
                "finished_at": finished_at.isoformat(),
                "duration_seconds": duration_seconds(started_at, finished_at),
                "error_summary": str(exc),
            }
        )
        if client is not None and audit_table_ready:
            try:
                audit_errors = _write_audit_row(client, audit_table_id=audit_table_id, entry=manifest_entry)
                if audit_errors:
                    logger.warning("Audit row insert returned errors during failure handling: %s", audit_errors)
            except Exception:
                logger.exception("Failed to write FX BigQuery audit row during failure handling run_id=%s", run_id)
        append_manifest(MANIFEST_PATH, manifest_entry)
        logger.exception("FX BigQuery load failed run_id=%s", run_id)
        raise


def main() -> None:
    parser = argparse.ArgumentParser(description="Load FX monthly silver parquet from GCS into BigQuery raw tables.")
    parser.add_argument("--source", choices=("gcs", "local"), default="gcs")
    parser.add_argument("--local-monthly-root", default=str(DEFAULT_LOCAL_MONTHLY_ROOT))
    parser.add_argument("--monthly-gcs-prefix", default=None)
    parser.add_argument("--monthly-table-name", default="ecb_fx_eu_monthly")
    parser.add_argument("--append-only", action="store_true", help="Append selected months instead of replacing touched partitions.")
    parser.add_argument("--include-loaded-months", action="store_true", help="Reload months even when checksums are unchanged.")
    parser.add_argument("--year-month", action="append", default=None, help="Restrict the load to a specific YYYY-MM month.")
    parser.add_argument("--since-year-month", default=None, help="Restrict loads to months from YYYY-MM onward.")
    parser.add_argument("--until-year-month", default=None, help="Restrict loads to months through YYYY-MM.")
    parser.add_argument("--log-level", default="INFO")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    selected_year_months = {_parse_year_month(value) for value in (args.year_month or [])}
    since_year_month = _parse_year_month(args.since_year_month) if args.since_year_month else None
    until_year_month = _parse_year_month(args.until_year_month) if args.until_year_month else None

    summary = load_fx(
        source_mode=args.source,
        local_monthly_root=Path(args.local_monthly_root),
        monthly_gcs_prefix=args.monthly_gcs_prefix,
        monthly_table_name=args.monthly_table_name,
        replace_touched_partitions=not args.append_only,
        include_loaded_months=args.include_loaded_months,
        selected_year_months=selected_year_months,
        since_year_month=since_year_month,
        until_year_month=until_year_month,
        dry_run=args.dry_run,
        log_level=args.log_level,
    )
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
