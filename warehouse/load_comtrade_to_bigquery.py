from __future__ import annotations

import argparse
import json
import sys
from dataclasses import dataclass
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
    iter_progress,
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


LOGGER_NAME = "comtrade.load_bigquery"
LOG_DIR = PROJECT_ROOT / "logs" / "comtrade"
LOG_PATH = LOG_DIR / "load_comtrade_to_bigquery.log"
MANIFEST_PATH = LOG_DIR / "load_comtrade_to_bigquery_manifest.jsonl"
FACT_BATCH_MANIFEST_PATH = LOG_DIR / "load_comtrade_to_bigquery_batches.jsonl"
AUDIT_TABLE_NAME = "comtrade_load_audit"
STATE_TABLE_NAME = "comtrade_load_state"
DEFAULT_FACT_LOCAL_ROOT = PROJECT_ROOT / "data" / "silver" / "comtrade" / "comtrade_fact"
FACT_PARTITION_FILENAME = "comtrade_fact.parquet"
MAX_URIS_PER_LOAD_JOB = 5000


def _bigquery_imports():
    try:
        from google.api_core.exceptions import NotFound
        from google.cloud import bigquery
    except ModuleNotFoundError as exc:
        raise ModuleNotFoundError(
            "google-cloud-bigquery is required for Comtrade BigQuery load operations."
        ) from exc
    return bigquery, NotFound


@dataclass(frozen=True)
class FixedTableSpec:
    table_name: str
    local_path: Path
    gcs_parts: tuple[str, ...]
    clustering_fields: tuple[str, ...] = ()


FIXED_TABLE_SPECS = (
    FixedTableSpec(
        table_name="dim_country",
        local_path=PROJECT_ROOT / "data" / "silver" / "comtrade" / "dimensions" / "dim_country.parquet",
        gcs_parts=("silver", "comtrade", "dimensions", "dim_country.parquet"),
    ),
    FixedTableSpec(
        table_name="dim_time",
        local_path=PROJECT_ROOT / "data" / "silver" / "comtrade" / "dimensions" / "dim_time.parquet",
        gcs_parts=("silver", "comtrade", "dimensions", "dim_time.parquet"),
    ),
    FixedTableSpec(
        table_name="dim_commodity",
        local_path=PROJECT_ROOT / "data" / "silver" / "comtrade" / "dimensions" / "dim_commodity.parquet",
        gcs_parts=("silver", "comtrade", "dimensions", "dim_commodity.parquet"),
    ),
    FixedTableSpec(
        table_name="dim_trade_flow",
        local_path=PROJECT_ROOT / "data" / "silver" / "comtrade" / "dimensions" / "dim_trade_flow.parquet",
        gcs_parts=("silver", "comtrade", "dimensions", "dim_trade_flow.parquet"),
    ),
    FixedTableSpec(
        table_name="dim_chokepoint",
        local_path=PROJECT_ROOT / "data" / "silver" / "comtrade" / "dimensions" / "dim_chokepoint.parquet",
        gcs_parts=("silver", "comtrade", "dimensions", "dim_chokepoint.parquet"),
    ),
    FixedTableSpec(
        table_name="dim_country_ports",
        local_path=PROJECT_ROOT / "data" / "silver" / "comtrade" / "dimensions" / "dim_country_ports.parquet",
        gcs_parts=("silver", "comtrade", "dimensions", "dim_country_ports.parquet"),
        clustering_fields=("iso3",),
    ),
    FixedTableSpec(
        table_name="route_applicability",
        local_path=PROJECT_ROOT / "data" / "silver" / "comtrade" / "dimensions" / "bridge_country_route_applicability.parquet",
        gcs_parts=("silver", "comtrade", "dimensions", "bridge_country_route_applicability.parquet"),
        clustering_fields=("reporter_iso3", "partner_iso3"),
    ),
    FixedTableSpec(
        table_name="dim_trade_routes",
        local_path=PROJECT_ROOT / "data" / "silver" / "comtrade" / "dim_trade_routes.parquet",
        gcs_parts=("silver", "comtrade", "dim_trade_routes.parquet"),
        clustering_fields=("reporter_iso3", "partner_iso3"),
    ),
)


def _parse_year_month(value: str) -> str:
    parsed = date.fromisoformat(f"{value}-01")
    return parsed.strftime("%Y-%m")


def _month_from_parts(parts: tuple[str, ...]) -> date:
    year = None
    month = None
    for part in parts:
        if part.startswith("year="):
            year = int(part.split("=", 1)[1])
        elif part.startswith("month="):
            month = int(part.split("=", 1)[1])
    if year is None or month is None:
        raise ValueError(f"Could not infer year/month from {'/'.join(parts)}")
    return date(year, month, 1)


def _month_from_gcs_uri(uri: str) -> date:
    parsed = urlparse(uri)
    return _month_from_parts(tuple(part for part in parsed.path.strip("/").split("/") if part))


def _fact_partition_files(local_root: Path) -> list[Path]:
    return sorted(local_root.glob("year=*/month=*/reporter_iso3=*/cmd_code=*/flow_code=*/comtrade_fact.parquet"))


def _fact_gcs_partition_uris(config: GcpCloudConfig) -> list[str]:
    return list_blob_uris(
        bucket_name=config.gcs_bucket,
        prefix=config.blob_path("silver", "comtrade", "comtrade_fact"),
        project_id=config.gcp_project_id,
        suffix=FACT_PARTITION_FILENAME,
    )


def _matches_month_filters(
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


def _filter_fact_candidate_uris(
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
        if _matches_month_filters(
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
        bigquery.SchemaField("fact_table_id", "STRING"),
        bigquery.SchemaField("candidate_year_months", "STRING", mode="REPEATED"),
        bigquery.SchemaField("selected_year_months", "STRING", mode="REPEATED"),
        bigquery.SchemaField("skipped_loaded_year_months", "STRING", mode="REPEATED"),
        bigquery.SchemaField("candidate_fact_file_count", "INT64"),
        bigquery.SchemaField("selected_fact_file_count", "INT64"),
        bigquery.SchemaField("fixed_table_results_json", "STRING"),
        bigquery.SchemaField("output_rows_json", "STRING"),
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
        "fact_table_id": entry.get("fact_table_id"),
        "candidate_year_months": entry.get("candidate_year_months", []),
        "selected_year_months": entry.get("selected_year_months", []),
        "skipped_loaded_year_months": entry.get("skipped_loaded_year_months", []),
        "candidate_fact_file_count": entry.get("candidate_fact_file_count"),
        "selected_fact_file_count": entry.get("selected_fact_file_count"),
        "fixed_table_results_json": json.dumps(entry.get("fixed_table_results", {})),
        "output_rows_json": json.dumps(entry.get("output_rows", {})),
        "error_summary": entry.get("error_summary"),
    }
    return client.insert_rows_json(audit_table_id, [row])


def _selected_fact_months(
    *,
    fact_month_state: dict[str, LoadStateRecord],
    include_loaded_months: bool,
    table_exists: bool,
    client,
    state_checksums: dict[str, str],
) -> tuple[list[str], list[str]]:
    candidate_months = sorted(fact_month_state.keys())

    if include_loaded_months or not table_exists or client is None:
        return candidate_months, []

    selected_months = []
    skipped_months = []
    for month in candidate_months:
        prior_checksum = state_checksums.get(month)
        if prior_checksum is not None and prior_checksum == fact_month_state[month].source_checksum:
            skipped_months.append(month)
        else:
            selected_months.append(month)
    return selected_months, skipped_months


def _gcs_uri_for_fixed_spec(config: GcpCloudConfig, spec: FixedTableSpec) -> str:
    return config.gcs_uri(*spec.gcs_parts)


def _fact_month_state(config: GcpCloudConfig, candidate_uris: list[str], fact_table_id: str) -> dict[str, LoadStateRecord]:
    metadata_by_name = list_blob_metadata(
        bucket_name=config.gcs_bucket,
        prefix=config.blob_path("silver", "comtrade", "comtrade_fact"),
        project_id=config.gcp_project_id,
        suffix=FACT_PARTITION_FILENAME,
    )
    month_entries: dict[str, list[tuple[str, str]]] = {}
    month_uris: dict[str, list[str]] = {}
    for uri in candidate_uris:
        blob_name = blob_name_from_gcs_uri(uri)
        blob_metadata = metadata_by_name.get(blob_name)
        if blob_metadata is None:
            raise FileNotFoundError(f"Expected GCS object for Comtrade fact source {uri}")
        if not blob_metadata.md5_hash:
            raise ValueError(f"GCS object {uri} is missing an md5 checksum; cannot perform checksum-aware load.")
        month_key = _month_from_gcs_uri(uri).strftime("%Y-%m")
        month_entries.setdefault(month_key, []).append((uri, blob_metadata.md5_hash))
        month_uris.setdefault(month_key, []).append(uri)

    return {
        month_key: LoadStateRecord(
            entity_type="fact_partition",
            entity_key=month_key,
            source_checksum=composite_checksum(entries),
            checksum_kind="gcs_md5_composite_sha256",
            source_uris=tuple(sorted(month_uris[month_key])),
            target_table_id=fact_table_id,
        )
        for month_key, entries in month_entries.items()
    }


def _chunked(items: tuple[str, ...], chunk_size: int) -> list[tuple[str, ...]]:
    return [items[index : index + chunk_size] for index in range(0, len(items), chunk_size)]


def _fixed_table_state(
    *,
    config: GcpCloudConfig,
    fixed_specs: tuple[FixedTableSpec, ...],
) -> dict[str, LoadStateRecord]:
    state: dict[str, LoadStateRecord] = {}
    for spec in fixed_specs:
        uri = _gcs_uri_for_fixed_spec(config, spec)
        blob_name = blob_name_from_gcs_uri(uri)
        blob_metadata = list_blob_metadata(
            bucket_name=config.gcs_bucket,
            prefix=blob_name,
            project_id=config.gcp_project_id,
        ).get(blob_name)
        if blob_metadata is None:
            raise FileNotFoundError(f"Expected GCS object for Comtrade fixed table source {uri}")
        if not blob_metadata.md5_hash:
            raise ValueError(f"GCS object {uri} is missing an md5 checksum; cannot perform checksum-aware load.")
        state[spec.table_name] = LoadStateRecord(
            entity_type="fixed_table",
            entity_key=spec.table_name,
            source_checksum=blob_metadata.md5_hash,
            checksum_kind="gcs_md5",
            source_uris=(uri,),
            target_table_id=f"{config.gcp_project_id}.{config.bq_raw_dataset}.{spec.table_name}",
        )
    return state


def load_comtrade(
    *,
    source_mode: str,
    local_fact_root: Path,
    replace_touched_partitions: bool,
    include_loaded_months: bool,
    selected_year_months: set[str],
    since_year_month: str | None,
    until_year_month: str | None,
    load_fixed_tables: bool,
    dry_run: bool,
    log_level: str,
) -> dict[str, object]:
    config = GcpCloudConfig.from_env()
    fact_table_id = f"{config.gcp_project_id}.{config.bq_raw_dataset}.comtrade_fact"
    state_table_id = f"{config.gcp_project_id}.{config.bq_raw_dataset}.{STATE_TABLE_NAME}"
    run_id = build_run_id("comtrade_load_bigquery")
    logger = configure_logger(
        logger_name=LOGGER_NAME,
        log_path=LOG_PATH,
        log_level=log_level,
    )
    started_at = datetime.now(timezone.utc)

    manifest_entry: dict[str, object] = {
        "run_id": run_id,
        "asset_name": "load_comtrade_to_bigquery",
        "dataset_name": "comtrade",
        "status": "running",
        "started_at": started_at.isoformat(),
        "finished_at": None,
        "duration_seconds": None,
        "fact_table_id": fact_table_id,
        "candidate_year_months": [],
        "selected_year_months": [],
        "skipped_loaded_year_months": [],
        "candidate_fact_file_count": 0,
        "selected_fact_file_count": 0,
        "fixed_table_results": {},
        "output_rows": {},
        "state_table_id": state_table_id,
        "skipped_unchanged_year_months": [],
        "skipped_unchanged_fixed_tables": [],
        "fact_batch_count": 0,
        "fact_batches_loaded": 0,
        "error_summary": None,
    }

    client = None
    audit_table_ready = False
    audit_table_id = None

    try:
        logger.info("Step 1/5 Discover candidate Comtrade fact parquet partitions")
        if source_mode == "local":
            partition_files = _fact_partition_files(local_fact_root)
            if not partition_files:
                raise FileNotFoundError(f"No Comtrade fact partition files found under {local_fact_root}")
            candidate_uris = [
                f"gs://{config.gcs_bucket}/{config.blob_path('silver', 'comtrade', 'comtrade_fact', path.relative_to(local_fact_root).as_posix())}"
                for path in partition_files
            ]
        else:
            candidate_uris = _fact_gcs_partition_uris(config)
            if not candidate_uris:
                raise FileNotFoundError(
                    f"No Comtrade fact parquet objects found under gs://{config.gcs_bucket}/{config.blob_path('silver', 'comtrade', 'comtrade_fact')}"
                )

        filtered_candidate_uris, candidate_months = _filter_fact_candidate_uris(
            candidate_uris=candidate_uris,
            selected_year_months=selected_year_months,
            since_year_month=since_year_month,
            until_year_month=until_year_month,
        )
        candidate_year_months = [month.strftime("%Y-%m") for month in candidate_months]
        manifest_entry["candidate_year_months"] = candidate_year_months
        manifest_entry["candidate_fact_file_count"] = len(filtered_candidate_uris)
        fact_month_state = _fact_month_state(config, filtered_candidate_uris, fact_table_id)
        logger.info(
            "Discovered candidate_fact_files=%s candidate_year_months=%s",
            len(filtered_candidate_uris),
            candidate_year_months,
        )

        summary: dict[str, object] = {
            "run_id": run_id,
            "fact_table_id": fact_table_id,
            "candidate_year_months": candidate_year_months,
            "candidate_fact_file_count": len(filtered_candidate_uris),
            "selected_year_months_filter": sorted(selected_year_months),
            "since_year_month": since_year_month,
            "until_year_month": until_year_month,
            "load_fixed_tables": load_fixed_tables,
            "dry_run": dry_run,
            "log_path": str(LOG_PATH),
            "manifest_path": str(MANIFEST_PATH),
            "fact_batch_manifest_path": str(FACT_BATCH_MANIFEST_PATH),
            "state_table_id": state_table_id,
        }

        fixed_table_uris = (
            {
                spec.table_name: _gcs_uri_for_fixed_spec(config, spec)
                for spec in FIXED_TABLE_SPECS
                if source_mode != "local" or spec.local_path.exists()
            }
            if load_fixed_tables
            else {}
        )

        if dry_run:
            summary["status"] = "planned"
            summary["fact_source_uris"] = filtered_candidate_uris[:20]
            summary["fixed_table_uris"] = fixed_table_uris
            finished_at = datetime.now(timezone.utc)
            summary["duration_seconds"] = duration_seconds(started_at, finished_at)
            manifest_entry["status"] = "planned"
            manifest_entry["finished_at"] = finished_at.isoformat()
            manifest_entry["duration_seconds"] = duration_seconds(started_at, finished_at)
            append_manifest(MANIFEST_PATH, manifest_entry)
            return json_ready(summary)

        logger.info("Step 2/5 Resolve BigQuery dataset and selected year-month partitions")
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
            client.get_table(fact_table_id)
        except NotFound:
            table_exists = False

        fact_state_checksums = fetch_load_state_checksums(
            client,
            bigquery=bigquery,
            state_table_id=state_table_id,
            entity_type="fact_partition",
            entity_keys=sorted(fact_month_state.keys()),
            location=config.gcp_location,
        )
        selected_year_months_list, skipped_year_months_list = _selected_fact_months(
            fact_month_state=fact_month_state,
            include_loaded_months=include_loaded_months,
            table_exists=table_exists,
            client=client,
            state_checksums=fact_state_checksums,
        )
        selected_uris = [uri for month in selected_year_months_list for uri in fact_month_state[month].source_uris]
        selected_months = [date.fromisoformat(f"{month}-01") for month in selected_year_months_list]
        selected_month_uri_count = {
            month: len(fact_month_state[month].source_uris) for month in selected_year_months_list
        }
        total_fact_batches = sum(
            len(_chunked(fact_month_state[month].source_uris, MAX_URIS_PER_LOAD_JOB))
            for month in selected_year_months_list
        )
        manifest_entry["selected_year_months"] = selected_year_months_list
        manifest_entry["skipped_loaded_year_months"] = skipped_year_months_list
        manifest_entry["skipped_unchanged_year_months"] = skipped_year_months_list
        manifest_entry["selected_fact_file_count"] = len(selected_uris)
        manifest_entry["fact_batch_count"] = total_fact_batches
        summary["selected_year_months"] = selected_year_months_list
        summary["skipped_loaded_year_months"] = skipped_year_months_list
        summary["skipped_unchanged_year_months"] = skipped_year_months_list
        summary["fact_batch_count"] = total_fact_batches
        logger.info(
            "Selected fact files=%s selected_changed_year_months=%s skipped_unchanged=%s fact_batches=%s",
            len(selected_uris),
            selected_year_months_list,
            skipped_year_months_list,
            total_fact_batches,
        )

        if selected_uris:
            logger.info("Step 3/5 Apply partition replacement policy for fact table")
            if table_exists and replace_touched_partitions:
                logger.info("Deleting touched ref_date partitions from %s", fact_table_id)
                delete_sql = f"""
                delete from `{fact_table_id}`
                where ref_date in unnest(@touched_month_start_dates)
                """
                delete_job = client.query(
                    delete_sql,
                    location=config.gcp_location,
                    job_config=bigquery.QueryJobConfig(
                        query_parameters=[
                            bigquery.ArrayQueryParameter(
                                "touched_month_start_dates",
                                "DATE",
                                selected_months,
                            )
                        ]
                    ),
                )
                delete_job.result()

            logger.info(
                "Step 4/5 Submit Comtrade fact load jobs months=%s total_uris=%s total_batches=%s",
                selected_year_months_list,
                len(selected_uris),
                total_fact_batches,
            )
            loaded_batch_count = 0
            month_iterable = iter_progress(
                selected_year_months_list,
                desc="BQ fact months",
                total=len(selected_year_months_list),
                unit="month",
            )
            for month_key in month_iterable:
                month_uris = fact_month_state[month_key].source_uris
                month_batches = _chunked(month_uris, MAX_URIS_PER_LOAD_JOB)
                logger.info(
                    "Loading Comtrade fact month=%s uri_count=%s batch_count=%s",
                    month_key,
                    selected_month_uri_count[month_key],
                    len(month_batches),
                )
                for batch_index, batch_uris in enumerate(month_batches, start=1):
                    batch_started_at = datetime.now(timezone.utc)
                    logger.info(
                        "Submitting Comtrade fact month=%s batch=%s/%s uris=%s",
                        month_key,
                        batch_index,
                        len(month_batches),
                        len(batch_uris),
                    )
                    load_job = None
                    try:
                        load_job = client.load_table_from_uri(
                            list(batch_uris),
                            fact_table_id,
                            location=config.gcp_location,
                            job_config=bigquery.LoadJobConfig(
                                source_format=bigquery.SourceFormat.PARQUET,
                                create_disposition=bigquery.CreateDisposition.CREATE_IF_NEEDED,
                                write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
                                time_partitioning=bigquery.TimePartitioning(
                                    type_=bigquery.TimePartitioningType.DAY,
                                    field="ref_date",
                                ),
                                clustering_fields=["reporter_iso3", "cmdCode", "flowCode"],
                                schema_update_options=[bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION],
                            ),
                        )
                        load_job.result()
                        batch_finished_at = datetime.now(timezone.utc)
                        loaded_batch_count += 1
                        batch_entry = {
                            "run_id": run_id,
                            "asset_name": "load_comtrade_to_bigquery_batch",
                            "dataset_name": "comtrade",
                            "fact_table_id": fact_table_id,
                            "year_month": month_key,
                            "batch_number": batch_index,
                            "batch_count": len(month_batches),
                            "source_uri_count": len(batch_uris),
                            "source_uri_sample": list(batch_uris[:10]),
                            "started_at": batch_started_at.isoformat(),
                            "finished_at": batch_finished_at.isoformat(),
                            "duration_seconds": duration_seconds(batch_started_at, batch_finished_at),
                            "status": "loaded",
                            "job_id": load_job.job_id,
                            "error_summary": None,
                        }
                        append_manifest(FACT_BATCH_MANIFEST_PATH, batch_entry)
                        logger.info(
                            "Finished Comtrade fact month=%s batch=%s/%s job_id=%s duration_s=%.3f",
                            month_key,
                            batch_index,
                            len(month_batches),
                            load_job.job_id,
                            batch_entry["duration_seconds"],
                        )
                    except Exception as batch_exc:
                        batch_finished_at = datetime.now(timezone.utc)
                        append_manifest(
                            FACT_BATCH_MANIFEST_PATH,
                            {
                                "run_id": run_id,
                                "asset_name": "load_comtrade_to_bigquery_batch",
                                "dataset_name": "comtrade",
                                "fact_table_id": fact_table_id,
                                "year_month": month_key,
                                "batch_number": batch_index,
                                "batch_count": len(month_batches),
                                "source_uri_count": len(batch_uris),
                                "source_uri_sample": list(batch_uris[:10]),
                                "started_at": batch_started_at.isoformat(),
                                "finished_at": batch_finished_at.isoformat(),
                                "duration_seconds": duration_seconds(batch_started_at, batch_finished_at),
                                "status": "failed",
                                "job_id": load_job.job_id if load_job is not None else None,
                                "error_summary": str(batch_exc),
                            },
                        )
                        raise
                replace_load_state_rows(
                    client,
                    bigquery=bigquery,
                    state_table_id=state_table_id,
                    rows=[fact_month_state[month_key]],
                    run_id=run_id,
                    source_mode=source_mode,
                    location=config.gcp_location,
                )
            manifest_entry["fact_batches_loaded"] = loaded_batch_count
            summary["fact_batches_loaded"] = loaded_batch_count

        fixed_table_results: dict[str, object] = {}
        if load_fixed_tables:
            logger.info("Step 5/5 Load fixed Comtrade tables")
            fixed_table_state = _fixed_table_state(config=config, fixed_specs=FIXED_TABLE_SPECS)
            fixed_table_checksums = fetch_load_state_checksums(
                client,
                bigquery=bigquery,
                state_table_id=state_table_id,
                entity_type="fixed_table",
                entity_keys=[spec.table_name for spec in FIXED_TABLE_SPECS],
                location=config.gcp_location,
            )
            selected_fixed_specs = [
                spec
                for spec in FIXED_TABLE_SPECS
                if fixed_table_checksums.get(spec.table_name) != fixed_table_state[spec.table_name].source_checksum
            ]
            skipped_fixed_tables = [
                spec.table_name
                for spec in FIXED_TABLE_SPECS
                if spec not in selected_fixed_specs
            ]
            manifest_entry["skipped_unchanged_fixed_tables"] = skipped_fixed_tables
            summary["skipped_unchanged_fixed_tables"] = skipped_fixed_tables
            for spec in iter_progress(
                selected_fixed_specs,
                desc="BQ fixed tables",
                total=len(selected_fixed_specs),
                unit="table",
            ):
                if source_mode == "local" and not spec.local_path.exists():
                    raise FileNotFoundError(f"Missing required Comtrade fixed table source: {spec.local_path}")
                table_id = f"{config.gcp_project_id}.{config.bq_raw_dataset}.{spec.table_name}"
                logger.info(
                    "Replacing fixed table=%s from %s with WRITE_TRUNCATE",
                    spec.table_name,
                    _gcs_uri_for_fixed_spec(config, spec),
                )
                load_job = client.load_table_from_uri(
                    _gcs_uri_for_fixed_spec(config, spec),
                    table_id,
                    location=config.gcp_location,
                    job_config=bigquery.LoadJobConfig(
                        source_format=bigquery.SourceFormat.PARQUET,
                        create_disposition=bigquery.CreateDisposition.CREATE_IF_NEEDED,
                        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
                        clustering_fields=list(spec.clustering_fields) or None,
                    ),
                )
                load_job.result()
                fixed_table_results[spec.table_name] = {"status": "loaded", "uri": _gcs_uri_for_fixed_spec(config, spec)}
            for table_name in skipped_fixed_tables:
                fixed_table_results[table_name] = {
                    "status": "skipped_unchanged",
                    "uri": fixed_table_state[table_name].source_uris[0],
                }
            replace_load_state_rows(
                client,
                bigquery=bigquery,
                state_table_id=state_table_id,
                rows=[fixed_table_state[spec.table_name] for spec in selected_fixed_specs],
                run_id=run_id,
                source_mode=source_mode,
                location=config.gcp_location,
            )

        output_rows: dict[str, object] = {}
        if selected_uris or table_exists:
            output_rows["comtrade_fact"] = client.get_table(fact_table_id).num_rows
        else:
            output_rows["comtrade_fact"] = None
        for spec in FIXED_TABLE_SPECS:
            if spec.table_name in fixed_table_results:
                output_rows[spec.table_name] = client.get_table(
                    f"{config.gcp_project_id}.{config.bq_raw_dataset}.{spec.table_name}"
                ).num_rows

        manifest_entry["fixed_table_results"] = fixed_table_results
        manifest_entry["output_rows"] = output_rows
        manifest_entry["status"] = "loaded"
        finished_at = datetime.now(timezone.utc)
        manifest_entry["finished_at"] = finished_at.isoformat()
        manifest_entry["duration_seconds"] = duration_seconds(started_at, finished_at)
        _write_audit_row(client, audit_table_id=audit_table_id, entry=manifest_entry)
        append_manifest(MANIFEST_PATH, manifest_entry)

        summary["status"] = "loaded"
        summary["fixed_table_results"] = fixed_table_results
        summary["output_rows"] = output_rows
        summary["duration_seconds"] = manifest_entry["duration_seconds"]
        logger.info(
            "Finished Comtrade BigQuery load run_id=%s duration_s=%.3f",
            run_id,
            manifest_entry["duration_seconds"],
        )
        return json_ready(summary)
    except Exception as exc:
        manifest_entry["status"] = "failed"
        finished_at = datetime.now(timezone.utc)
        manifest_entry["finished_at"] = finished_at.isoformat()
        manifest_entry["duration_seconds"] = duration_seconds(started_at, finished_at)
        manifest_entry["error_summary"] = str(exc)
        if client is not None and audit_table_ready and audit_table_id is not None:
            try:
                _write_audit_row(client, audit_table_id=audit_table_id, entry=manifest_entry)
            except Exception:
                logger.exception("Failed to write Comtrade load audit row during failure handling.")
        append_manifest(MANIFEST_PATH, manifest_entry)
        logger.exception("Comtrade BigQuery load failed run_id=%s", run_id)
        raise


def _build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Load Comtrade silver fact and route support tables from GCS into BigQuery raw.*.")
    parser.add_argument("--source", choices=("gcs", "local"), default="gcs")
    parser.add_argument("--local-fact-root", default=str(DEFAULT_FACT_LOCAL_ROOT))
    parser.add_argument("--year-month", action="append", default=None)
    parser.add_argument("--since-year-month", default=None)
    parser.add_argument("--until-year-month", default=None)
    parser.add_argument("--include-loaded-months", action="store_true", help="Reload fact months even if they already exist in BigQuery.")
    parser.add_argument("--append-only", action="store_true", help="Append selected fact files without deleting touched partitions first.")
    parser.add_argument("--skip-fixed-tables", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--log-level", default="INFO")
    return parser


def main() -> None:
    parser = _build_arg_parser()
    args = parser.parse_args()

    selected_year_months = {_parse_year_month(value) for value in (args.year_month or [])}
    since_year_month = _parse_year_month(args.since_year_month) if args.since_year_month else None
    until_year_month = _parse_year_month(args.until_year_month) if args.until_year_month else None

    summary = load_comtrade(
        source_mode=args.source,
        local_fact_root=Path(args.local_fact_root),
        replace_touched_partitions=not args.append_only,
        include_loaded_months=args.include_loaded_months,
        selected_year_months=selected_year_months,
        since_year_month=since_year_month,
        until_year_month=until_year_month,
        load_fixed_tables=not args.skip_fixed_tables,
        dry_run=args.dry_run,
        log_level=args.log_level,
    )
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
