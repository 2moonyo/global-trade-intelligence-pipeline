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

from google.api_core.exceptions import NotFound
from google.cloud import bigquery

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
    composite_checksum,
    ensure_load_state_table,
    fetch_load_state_checksums,
    replace_load_state_rows,
)


DEFAULT_LOCAL_DAILY_ROOT = PROJECT_ROOT / "data" / "silver" / "portwatch" / "portwatch_daily"
DEFAULT_LOCAL_MONTHLY_ROOT = PROJECT_ROOT / "data" / "silver" / "portwatch" / "portwatch_monthly"
DEFAULT_DAILY_GCS_PREFIX_PARTS = ("silver", "portwatch", "portwatch_daily")
DEFAULT_MONTHLY_GCS_PREFIX_PARTS = ("silver", "portwatch", "portwatch_monthly")
DAILY_PARTITION_FILENAME = "portwatch_daily.parquet"
MONTHLY_PARTITION_FILENAME = "portwatch_monthly.parquet"
LOGGER_NAME = "portwatch.load_bigquery"
LOG_DIR = PROJECT_ROOT / "logs" / "portwatch"
LOG_PATH = LOG_DIR / "load_portwatch_to_bigquery.log"
MANIFEST_PATH = LOG_DIR / "load_portwatch_to_bigquery_manifest.jsonl"
AUDIT_TABLE_NAME = "portwatch_load_audit"
STATE_TABLE_NAME = "portwatch_load_state"


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


def _partition_files(local_root: Path, partition_filename: str) -> list[Path]:
    return sorted(local_root.glob(f"year=*/month=*/{partition_filename}"))


def _gcs_partition_uris(config: GcpCloudConfig, gcs_prefix: str, partition_filename: str) -> list[str]:
    return list_blob_uris(
        bucket_name=config.gcs_bucket,
        prefix=gcs_prefix,
        project_id=config.gcp_project_id,
        suffix=partition_filename,
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


def _ensure_audit_table(
    client: bigquery.Client,
    *,
    project_id: str,
    dataset_name: str,
) -> str:
    table_id = f"{project_id}.{dataset_name}.{AUDIT_TABLE_NAME}"
    schema = [
        bigquery.SchemaField("run_id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("started_at", "TIMESTAMP"),
        bigquery.SchemaField("finished_at", "TIMESTAMP"),
        bigquery.SchemaField("status", "STRING"),
        bigquery.SchemaField("source_mode", "STRING"),
        bigquery.SchemaField("daily_table_id", "STRING"),
        bigquery.SchemaField("monthly_table_id", "STRING"),
        bigquery.SchemaField("daily_gcs_prefix", "STRING"),
        bigquery.SchemaField("monthly_gcs_prefix", "STRING"),
        bigquery.SchemaField("candidate_year_months", "STRING", mode="REPEATED"),
        bigquery.SchemaField("selected_year_months", "STRING", mode="REPEATED"),
        bigquery.SchemaField("skipped_loaded_year_months", "STRING", mode="REPEATED"),
        bigquery.SchemaField("daily_candidate_file_count", "INT64"),
        bigquery.SchemaField("monthly_candidate_file_count", "INT64"),
        bigquery.SchemaField("daily_selected_file_count", "INT64"),
        bigquery.SchemaField("monthly_selected_file_count", "INT64"),
        bigquery.SchemaField("replace_touched_partitions", "BOOL"),
        bigquery.SchemaField("include_loaded_months", "BOOL"),
        bigquery.SchemaField("output_rows_json", "STRING"),
        bigquery.SchemaField("error_summary", "STRING"),
    ]
    table = bigquery.Table(table_id, schema=schema)
    client.create_table(table, exists_ok=True)
    return table_id


def _write_audit_row(
    client: bigquery.Client,
    *,
    audit_table_id: str,
    entry: dict[str, object],
) -> list[dict[str, object]]:
    row = {
        "run_id": entry.get("run_id"),
        "started_at": entry.get("started_at"),
        "finished_at": entry.get("finished_at"),
        "status": entry.get("status"),
        "source_mode": entry.get("source_mode"),
        "daily_table_id": entry.get("daily_table_id"),
        "monthly_table_id": entry.get("monthly_table_id"),
        "daily_gcs_prefix": entry.get("daily_gcs_prefix"),
        "monthly_gcs_prefix": entry.get("monthly_gcs_prefix"),
        "candidate_year_months": entry.get("candidate_year_months", []),
        "selected_year_months": entry.get("selected_year_months", []),
        "skipped_loaded_year_months": entry.get("skipped_loaded_year_months", []),
        "daily_candidate_file_count": entry.get("daily_candidate_file_count"),
        "monthly_candidate_file_count": entry.get("monthly_candidate_file_count"),
        "daily_selected_file_count": entry.get("daily_selected_file_count"),
        "monthly_selected_file_count": entry.get("monthly_selected_file_count"),
        "replace_touched_partitions": entry.get("replace_touched_partitions"),
        "include_loaded_months": entry.get("include_loaded_months"),
        "output_rows_json": json.dumps(entry.get("output_rows", {})),
        "error_summary": entry.get("error_summary"),
    }
    return client.insert_rows_json(audit_table_id, [row])


def _source_state_by_month(
    *,
    config: GcpCloudConfig,
    resolved_gcs_prefix: str,
    partition_filename: str,
    candidate_uris: list[str],
    entity_type: str,
    table_id: str,
) -> dict[str, LoadStateRecord]:
    metadata_by_name = list_blob_metadata(
        bucket_name=config.gcs_bucket,
        prefix=resolved_gcs_prefix,
        project_id=config.gcp_project_id,
        suffix=partition_filename,
    )
    month_entries: dict[str, list[tuple[str, str]]] = {}
    month_uris: dict[str, list[str]] = {}
    for uri in candidate_uris:
        blob_name = blob_name_from_gcs_uri(uri)
        blob_metadata = metadata_by_name.get(blob_name)
        if blob_metadata is None:
            raise FileNotFoundError(f"Expected GCS object for PortWatch load source {uri}")
        if not blob_metadata.md5_hash:
            raise ValueError(f"GCS object {uri} is missing an md5 checksum; cannot perform checksum-aware load.")
        month_key = _month_from_gcs_uri(uri).strftime("%Y-%m")
        month_entries.setdefault(month_key, []).append((uri, blob_metadata.md5_hash))
        month_uris.setdefault(month_key, []).append(uri)

    return {
        month_key: LoadStateRecord(
            entity_type=entity_type,
            entity_key=month_key,
            source_checksum=composite_checksum(entries),
            checksum_kind="gcs_md5_composite_sha256",
            source_uris=tuple(sorted(month_uris[month_key])),
            target_table_id=table_id,
        )
        for month_key, entries in month_entries.items()
    }


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


def _submit_load_if_needed(
    *,
    selected_months: list[str],
    month_state: dict[str, LoadStateRecord],
    table_exists: bool,
    replace_touched_partitions: bool,
    delete_sql: str,
    delete_parameter_name: str,
    table_id: str,
    partition_field: str,
    clustering_fields: list[str],
    location: str,
    client: bigquery.Client,
) -> None:
    if not selected_months:
        return

    selected_dates = [date.fromisoformat(f"{value}-01") for value in selected_months]
    if table_exists and replace_touched_partitions:
        delete_job = client.query(
            delete_sql.format(table_id=table_id, delete_parameter_name=delete_parameter_name),
            location=location,
            job_config=bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ArrayQueryParameter(delete_parameter_name, "DATE", selected_dates),
                ]
            ),
        )
        delete_job.result()

    selected_uris = [uri for month in selected_months for uri in month_state[month].source_uris]
    load_job = client.load_table_from_uri(
        selected_uris,
        table_id,
        location=location,
        job_config=bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.PARQUET,
            create_disposition=bigquery.CreateDisposition.CREATE_IF_NEEDED,
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
            schema_update_options=[bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION],
            time_partitioning=bigquery.TimePartitioning(
                type_=bigquery.TimePartitioningType.DAY,
                field=partition_field,
            ),
            clustering_fields=clustering_fields,
        ),
    )
    load_job.result()


def load_portwatch(
    *,
    source_mode: str,
    local_daily_root: Path,
    local_monthly_root: Path,
    daily_gcs_prefix: str | None,
    monthly_gcs_prefix: str | None,
    daily_table_name: str,
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
    resolved_daily_prefix = daily_gcs_prefix or config.blob_path(*DEFAULT_DAILY_GCS_PREFIX_PARTS)
    resolved_monthly_prefix = monthly_gcs_prefix or config.blob_path(*DEFAULT_MONTHLY_GCS_PREFIX_PARTS)
    daily_table_id = f"{config.gcp_project_id}.{config.bq_raw_dataset}.{daily_table_name}"
    monthly_table_id = f"{config.gcp_project_id}.{config.bq_raw_dataset}.{monthly_table_name}"
    audit_table_id = f"{config.gcp_project_id}.{config.bq_raw_dataset}.{AUDIT_TABLE_NAME}"
    state_table_id = f"{config.gcp_project_id}.{config.bq_raw_dataset}.{STATE_TABLE_NAME}"
    run_id = build_run_id("portwatch_load_bigquery")
    logger = configure_logger(
        logger_name=LOGGER_NAME,
        log_path=LOG_PATH,
        log_level=log_level,
    )
    started_at = datetime.now(timezone.utc)

    manifest_entry: dict[str, object] = {
        "run_id": run_id,
        "asset_name": "load_portwatch_to_bigquery",
        "dataset_name": "portwatch",
        "status": "running",
        "started_at": started_at.isoformat(),
        "finished_at": None,
        "duration_seconds": None,
        "source_mode": source_mode,
        "daily_table_id": daily_table_id,
        "monthly_table_id": monthly_table_id,
        "daily_gcs_prefix": resolved_daily_prefix,
        "monthly_gcs_prefix": resolved_monthly_prefix,
        "replace_touched_partitions": replace_touched_partitions,
        "include_loaded_months": include_loaded_months,
        "selected_year_months_filter": sorted(selected_year_months),
        "since_year_month": since_year_month,
        "until_year_month": until_year_month,
        "candidate_year_months": [],
        "selected_year_months": [],
        "skipped_loaded_year_months": [],
        "daily_candidate_file_count": 0,
        "monthly_candidate_file_count": 0,
        "daily_selected_file_count": 0,
        "monthly_selected_file_count": 0,
        "output_rows": {},
        "audit_table_id": audit_table_id,
        "state_table_id": state_table_id,
        "error_summary": None,
        "dry_run": dry_run,
    }

    client: bigquery.Client | None = None
    audit_table_ready = False

    try:
        logger.info("Starting PortWatch BigQuery load run_id=%s", run_id)
        logger.info("Step 1/4 Discover candidate PortWatch parquet partitions")

        if source_mode == "local":
            daily_candidate_uris = [
                _uri_from_resolved_prefix(
                    bucket_name=config.gcs_bucket,
                    resolved_prefix=resolved_daily_prefix,
                    relative_path=path.relative_to(local_daily_root).as_posix(),
                )
                for path in _partition_files(local_daily_root, DAILY_PARTITION_FILENAME)
            ]
            monthly_candidate_uris = [
                _uri_from_resolved_prefix(
                    bucket_name=config.gcs_bucket,
                    resolved_prefix=resolved_monthly_prefix,
                    relative_path=path.relative_to(local_monthly_root).as_posix(),
                )
                for path in _partition_files(local_monthly_root, MONTHLY_PARTITION_FILENAME)
            ]
        else:
            daily_candidate_uris = _gcs_partition_uris(config, resolved_daily_prefix, DAILY_PARTITION_FILENAME)
            monthly_candidate_uris = _gcs_partition_uris(config, resolved_monthly_prefix, MONTHLY_PARTITION_FILENAME)

        if not daily_candidate_uris:
            raise FileNotFoundError(f"No PortWatch daily parquet partitions found for prefix {resolved_daily_prefix}")
        if not monthly_candidate_uris:
            raise FileNotFoundError(f"No PortWatch monthly parquet partitions found for prefix {resolved_monthly_prefix}")

        filtered_daily_uris, daily_candidate_months = _filter_candidate_uris(
            candidate_uris=daily_candidate_uris,
            selected_year_months=selected_year_months,
            since_year_month=since_year_month,
            until_year_month=until_year_month,
        )
        filtered_monthly_uris, monthly_candidate_months = _filter_candidate_uris(
            candidate_uris=monthly_candidate_uris,
            selected_year_months=selected_year_months,
            since_year_month=since_year_month,
            until_year_month=until_year_month,
        )
        candidate_year_months = sorted(
            {
                month.strftime("%Y-%m")
                for month in [*daily_candidate_months, *monthly_candidate_months]
            }
        )
        if not filtered_daily_uris and not filtered_monthly_uris:
            raise FileNotFoundError("No PortWatch partition files matched the requested year-month filters.")

        summary: dict[str, object] = {
            "run_id": run_id,
            "project_id": config.gcp_project_id,
            "location": config.gcp_location,
            "source_mode": source_mode,
            "daily_table_id": daily_table_id,
            "monthly_table_id": monthly_table_id,
            "daily_gcs_prefix": resolved_daily_prefix,
            "monthly_gcs_prefix": resolved_monthly_prefix,
            "candidate_year_months": candidate_year_months,
            "daily_candidate_file_count": len(filtered_daily_uris),
            "monthly_candidate_file_count": len(filtered_monthly_uris),
            "selected_year_months_filter": sorted(selected_year_months),
            "since_year_month": since_year_month,
            "until_year_month": until_year_month,
            "replace_touched_partitions": replace_touched_partitions,
            "include_loaded_months": include_loaded_months,
            "dry_run": dry_run,
            "log_path": str(LOG_PATH),
            "manifest_path": str(MANIFEST_PATH),
            "audit_table_id": audit_table_id,
            "state_table_id": state_table_id,
        }
        manifest_entry["candidate_year_months"] = candidate_year_months
        manifest_entry["daily_candidate_file_count"] = len(filtered_daily_uris)
        manifest_entry["monthly_candidate_file_count"] = len(filtered_monthly_uris)

        if dry_run:
            finished_at = datetime.now(timezone.utc)
            summary["status"] = "planned"
            summary["daily_gcs_source_uris"] = filtered_daily_uris[:20]
            summary["monthly_gcs_source_uris"] = filtered_monthly_uris[:20]
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

        daily_state = _source_state_by_month(
            config=config,
            resolved_gcs_prefix=resolved_daily_prefix,
            partition_filename=DAILY_PARTITION_FILENAME,
            candidate_uris=filtered_daily_uris,
            entity_type="daily_partition",
            table_id=daily_table_id,
        )
        monthly_state = _source_state_by_month(
            config=config,
            resolved_gcs_prefix=resolved_monthly_prefix,
            partition_filename=MONTHLY_PARTITION_FILENAME,
            candidate_uris=filtered_monthly_uris,
            entity_type="monthly_partition",
            table_id=monthly_table_id,
        )

        logger.info("Step 2/4 Resolve BigQuery dataset and changed months")
        client = bigquery.Client(project=config.gcp_project_id)
        _ensure_dataset(
            client,
            project_id=config.gcp_project_id,
            dataset_name=config.bq_raw_dataset,
            location=config.gcp_location,
        )
        audit_table_id = _ensure_audit_table(
            client,
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

        daily_table_exists = True
        monthly_table_exists = True
        try:
            client.get_table(daily_table_id)
        except NotFound:
            daily_table_exists = False
        try:
            client.get_table(monthly_table_id)
        except NotFound:
            monthly_table_exists = False

        daily_checksums = fetch_load_state_checksums(
            client,
            bigquery=bigquery,
            state_table_id=state_table_id,
            entity_type="daily_partition",
            entity_keys=sorted(daily_state.keys()),
            location=config.gcp_location,
        )
        monthly_checksums = fetch_load_state_checksums(
            client,
            bigquery=bigquery,
            state_table_id=state_table_id,
            entity_type="monthly_partition",
            entity_keys=sorted(monthly_state.keys()),
            location=config.gcp_location,
        )
        selected_daily_months, skipped_daily_months = _selected_months(
            month_state=daily_state,
            include_loaded_months=include_loaded_months,
            state_checksums=daily_checksums,
            table_exists=daily_table_exists,
        )
        selected_monthly_months, skipped_monthly_months = _selected_months(
            month_state=monthly_state,
            include_loaded_months=include_loaded_months,
            state_checksums=monthly_checksums,
            table_exists=monthly_table_exists,
        )
        selected_year_month_values = sorted({*selected_daily_months, *selected_monthly_months})
        skipped_year_month_values = sorted({*skipped_daily_months, *skipped_monthly_months})

        summary["selected_year_months"] = selected_year_month_values
        summary["skipped_loaded_year_months"] = skipped_year_month_values
        summary["daily_selected_file_count"] = len(
            [uri for month in selected_daily_months for uri in daily_state[month].source_uris]
        )
        summary["monthly_selected_file_count"] = len(
            [uri for month in selected_monthly_months for uri in monthly_state[month].source_uris]
        )
        manifest_entry["selected_year_months"] = selected_year_month_values
        manifest_entry["skipped_loaded_year_months"] = skipped_year_month_values
        manifest_entry["daily_selected_file_count"] = summary["daily_selected_file_count"]
        manifest_entry["monthly_selected_file_count"] = summary["monthly_selected_file_count"]

        if not selected_daily_months and not selected_monthly_months:
            summary["status"] = "no_op_all_candidate_months_already_loaded"
            finished_at = datetime.now(timezone.utc)
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

        logger.info(
            "Candidate months=%s selected_daily=%s selected_monthly=%s skipped=%s",
            summary["candidate_year_months"],
            selected_daily_months,
            selected_monthly_months,
            skipped_year_month_values,
        )

        logger.info("Step 3/4 Delete touched partitions and submit load jobs")
        _submit_load_if_needed(
            selected_months=selected_daily_months,
            month_state=daily_state,
            table_exists=daily_table_exists,
            replace_touched_partitions=replace_touched_partitions,
            delete_sql="""
            delete from `{table_id}`
            where date_trunc(cast(date_day as date), month) in unnest(@{delete_parameter_name})
            """,
            delete_parameter_name="touched_month_start_dates",
            table_id=daily_table_id,
            partition_field="date_day",
            clustering_fields=["chokepoint_id"],
            location=config.gcp_location,
            client=client,
        )
        _submit_load_if_needed(
            selected_months=selected_monthly_months,
            month_state=monthly_state,
            table_exists=monthly_table_exists,
            replace_touched_partitions=replace_touched_partitions,
            delete_sql="""
            delete from `{table_id}`
            where cast(month_start_date as date) in unnest(@{delete_parameter_name})
            """,
            delete_parameter_name="touched_month_start_dates",
            table_id=monthly_table_id,
            partition_field="month_start_date",
            clustering_fields=["chokepoint_id"],
            location=config.gcp_location,
            client=client,
        )

        replace_load_state_rows(
            client,
            bigquery=bigquery,
            state_table_id=state_table_id,
            rows=[daily_state[month] for month in selected_daily_months]
            + [monthly_state[month] for month in selected_monthly_months],
            run_id=run_id,
            source_mode=source_mode,
            location=config.gcp_location,
        )

        logger.info("Step 4/4 Fetch landing table row counts")
        output_rows = {
            daily_table_name: client.get_table(daily_table_id).num_rows,
            monthly_table_name: client.get_table(monthly_table_id).num_rows,
        }
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
                logger.exception("Failed to write BigQuery audit row during failure handling run_id=%s", run_id)
        append_manifest(MANIFEST_PATH, manifest_entry)
        logger.exception("PortWatch BigQuery load failed run_id=%s", run_id)
        raise


def _build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Load PortWatch daily and monthly silver parquet partitions into BigQuery raw tables."
    )
    parser.add_argument(
        "--source",
        choices=("gcs", "local"),
        default="gcs",
        help="Read candidate partition files from GCS or derive GCS URIs from local silver trees.",
    )
    parser.add_argument(
        "--local-daily-root",
        default=str(DEFAULT_LOCAL_DAILY_ROOT),
        help="Local root for the canonical PortWatch daily silver partitions when --source local is used.",
    )
    parser.add_argument(
        "--local-monthly-root",
        default=str(DEFAULT_LOCAL_MONTHLY_ROOT),
        help="Local root for the canonical PortWatch monthly silver partitions when --source local is used.",
    )
    parser.add_argument(
        "--daily-gcs-prefix",
        default=None,
        help="Optional blob prefix inside the configured bucket for daily partitions. Defaults to silver/portwatch/portwatch_daily.",
    )
    parser.add_argument(
        "--monthly-gcs-prefix",
        default=None,
        help="Optional blob prefix inside the configured bucket for monthly partitions. Defaults to silver/portwatch/portwatch_monthly.",
    )
    parser.add_argument("--daily-table-name", default="portwatch_daily", help="BigQuery raw daily table name.")
    parser.add_argument("--monthly-table-name", default="portwatch_monthly", help="BigQuery raw monthly table name.")
    parser.add_argument(
        "--append-only",
        action="store_true",
        help="When loading selected months, skip the delete step and append rows into the landing tables.",
    )
    parser.add_argument(
        "--include-loaded-months",
        action="store_true",
        help="Reload months that are already present in BigQuery instead of skipping them.",
    )
    parser.add_argument(
        "--year-month",
        action="append",
        default=None,
        help="Restrict the load to a specific YYYY-MM month. Repeat for multiple months.",
    )
    parser.add_argument("--since-year-month", default=None, help="Restrict loads to months from YYYY-MM onward.")
    parser.add_argument("--until-year-month", default=None, help="Restrict loads to months through YYYY-MM.")
    parser.add_argument("--log-level", default="INFO", help="Logging level, for example INFO or DEBUG.")
    parser.add_argument("--dry-run", action="store_true", help="Show the intended load job without calling BigQuery.")
    return parser


def main() -> None:
    parser = _build_arg_parser()
    args = parser.parse_args()

    selected_year_months = {_parse_year_month(value) for value in (args.year_month or [])}
    since_year_month = _parse_year_month(args.since_year_month) if args.since_year_month else None
    until_year_month = _parse_year_month(args.until_year_month) if args.until_year_month else None

    summary = load_portwatch(
        source_mode=args.source,
        local_daily_root=Path(args.local_daily_root),
        local_monthly_root=Path(args.local_monthly_root),
        daily_gcs_prefix=args.daily_gcs_prefix,
        monthly_gcs_prefix=args.monthly_gcs_prefix,
        daily_table_name=args.daily_table_name,
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
