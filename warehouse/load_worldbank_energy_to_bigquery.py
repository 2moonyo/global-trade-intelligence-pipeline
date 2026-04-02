from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
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


DEFAULT_LOCAL_SILVER_ROOT = PROJECT_ROOT / "data" / "silver" / "worldbank_energy" / "energy_vulnerability"
DEFAULT_GCS_PREFIX_PARTS = ("silver", "worldbank_energy", "energy_vulnerability")
PARTITION_FILENAME = "energy_vulnerability.parquet"
LOGGER_NAME = "worldbank_energy.load_bigquery"
LOG_DIR = PROJECT_ROOT / "logs" / "worldbank_energy"
LOG_PATH = LOG_DIR / "load_worldbank_energy_to_bigquery.log"
MANIFEST_PATH = LOG_DIR / "load_worldbank_energy_to_bigquery_manifest.jsonl"
AUDIT_TABLE_NAME = "worldbank_energy_load_audit"
STATE_TABLE_NAME = "worldbank_energy_load_state"


def _bigquery_imports():
    try:
        from google.api_core.exceptions import NotFound
        from google.cloud import bigquery
    except ModuleNotFoundError as exc:
        raise ModuleNotFoundError(
            "google-cloud-bigquery is required for BigQuery load operations. "
            "Install it in the active environment before running this command."
        ) from exc
    return bigquery, NotFound


def _parse_year(value: str) -> str:
    year = int(value)
    if year < 1900 or year > 9999:
        raise argparse.ArgumentTypeError(f"Expected a four-digit year, got {value}")
    return f"{year:04d}"


def _year_from_partition_parts(parts: tuple[str, ...]) -> int:
    for part in parts:
        if part.startswith("year="):
            return int(part.split("=", 1)[1])
    raise ValueError(f"Could not infer year partition from {'/'.join(parts)}")


def _year_from_local_path(path: Path) -> int:
    return _year_from_partition_parts(path.parts)


def _year_from_gcs_uri(uri: str) -> int:
    parsed = urlparse(uri)
    return _year_from_partition_parts(tuple(part for part in parsed.path.strip("/").split("/") if part))


def _partition_files(local_silver_root: Path) -> list[Path]:
    return sorted(local_silver_root.glob(f"year=*/{PARTITION_FILENAME}"))


def _gcs_partition_uris(config: GcpCloudConfig, gcs_prefix: str) -> list[str]:
    return list_blob_uris(
        bucket_name=config.gcs_bucket,
        prefix=gcs_prefix,
        project_id=config.gcp_project_id,
        suffix=PARTITION_FILENAME,
    )


def _uri_from_resolved_prefix(*, bucket_name: str, resolved_prefix: str, relative_path: str) -> str:
    return f"gs://{bucket_name}/{resolved_prefix.strip('/')}/{relative_path.lstrip('/')}"


def _matches_year_filters(
    *,
    year_value: str,
    selected_years: set[str],
    since_year: str | None,
    until_year: str | None,
) -> bool:
    if selected_years and year_value not in selected_years:
        return False
    if since_year and year_value < since_year:
        return False
    if until_year and year_value > until_year:
        return False
    return True


def _filter_candidate_uris(
    *,
    candidate_uris: list[str],
    selected_years: set[str],
    since_year: str | None,
    until_year: str | None,
) -> tuple[list[str], list[int]]:
    filtered: list[str] = []
    years: list[int] = []
    for uri in candidate_uris:
        year_value = _year_from_gcs_uri(uri)
        year_text = f"{year_value:04d}"
        if _matches_year_filters(
            year_value=year_text,
            selected_years=selected_years,
            since_year=since_year,
            until_year=until_year,
        ):
            filtered.append(uri)
            years.append(year_value)
    return filtered, sorted(set(years))


def _ensure_dataset(
    client,
    *,
    bigquery,
    project_id: str,
    dataset_name: str,
    location: str,
) -> None:
    dataset = bigquery.Dataset(f"{project_id}.{dataset_name}")
    dataset.location = location
    client.create_dataset(dataset, exists_ok=True)


def _ensure_audit_table(
    client,
    *,
    bigquery,
    project_id: str,
    dataset_name: str,
) -> str:
    table_id = f"{project_id}.{dataset_name}.{AUDIT_TABLE_NAME}"
    schema = [
        bigquery.SchemaField("run_id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("started_at", "TIMESTAMP"),
        bigquery.SchemaField("finished_at", "TIMESTAMP"),
        bigquery.SchemaField("status", "STRING"),
        bigquery.SchemaField("table_id", "STRING"),
        bigquery.SchemaField("source_mode", "STRING"),
        bigquery.SchemaField("gcs_prefix", "STRING"),
        bigquery.SchemaField("candidate_file_count", "INT64"),
        bigquery.SchemaField("selected_file_count", "INT64"),
        bigquery.SchemaField("candidate_years", "STRING", mode="REPEATED"),
        bigquery.SchemaField("selected_years", "STRING", mode="REPEATED"),
        bigquery.SchemaField("skipped_loaded_years", "STRING", mode="REPEATED"),
        bigquery.SchemaField("replace_touched_partitions", "BOOL"),
        bigquery.SchemaField("include_loaded_years", "BOOL"),
        bigquery.SchemaField("output_rows", "INT64"),
        bigquery.SchemaField("error_summary", "STRING"),
    ]
    table = bigquery.Table(table_id, schema=schema)
    client.create_table(table, exists_ok=True)
    return table_id


def _write_audit_row(
    client,
    *,
    audit_table_id: str,
    entry: dict[str, object],
) -> list[dict[str, object]]:
    row = {
        "run_id": entry.get("run_id"),
        "started_at": entry.get("started_at"),
        "finished_at": entry.get("finished_at"),
        "status": entry.get("status"),
        "table_id": entry.get("table_id"),
        "source_mode": entry.get("source_mode"),
        "gcs_prefix": entry.get("gcs_prefix"),
        "candidate_file_count": entry.get("candidate_file_count"),
        "selected_file_count": entry.get("selected_file_count"),
        "candidate_years": entry.get("candidate_years", []),
        "selected_years": entry.get("selected_years", []),
        "skipped_loaded_years": entry.get("skipped_loaded_years", []),
        "replace_touched_partitions": entry.get("replace_touched_partitions"),
        "include_loaded_years": entry.get("include_loaded_years"),
        "output_rows": entry.get("output_rows"),
        "error_summary": entry.get("error_summary"),
    }
    return client.insert_rows_json(audit_table_id, [row])


def _existing_loaded_years(
    client,
    *,
    bigquery,
    table_id: str,
    location: str,
    candidate_years: list[int],
) -> set[int]:
    if not candidate_years:
        return set()

    query = f"""
    select distinct year
    from `{table_id}`
    where year in unnest(@candidate_years)
    """
    job = client.query(
        query,
        location=location,
        job_config=bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ArrayQueryParameter("candidate_years", "INT64", candidate_years),
            ]
        ),
    )
    return {int(row["year"]) for row in job.result() if row["year"] is not None}


def _selected_uris_by_year(
    *,
    year_state: dict[str, LoadStateRecord],
    include_loaded_years: bool,
    state_checksums: dict[str, str],
    client,
    table_exists: bool,
) -> tuple[list[str], list[str]]:
    candidate_years = sorted(year_state.keys())

    if include_loaded_years or not table_exists or client is None:
        return candidate_years, []

    selected_years = []
    skipped_years = []
    for year in candidate_years:
        prior_checksum = state_checksums.get(year)
        if prior_checksum is not None and prior_checksum == year_state[year].source_checksum:
            skipped_years.append(year)
        else:
            selected_years.append(year)
    return selected_years, skipped_years


def _source_state_by_year(
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
    year_state: dict[str, LoadStateRecord] = {}
    for uri in candidate_uris:
        blob_name = blob_name_from_gcs_uri(uri)
        blob_metadata = metadata_by_name.get(blob_name)
        if blob_metadata is None:
            raise FileNotFoundError(f"Expected GCS object for World Bank energy load source {uri}")
        if not blob_metadata.md5_hash:
            raise ValueError(f"GCS object {uri} is missing an md5 checksum; cannot perform checksum-aware load.")
        year_key = f"{_year_from_gcs_uri(uri):04d}"
        year_state[year_key] = LoadStateRecord(
            entity_type="fact_partition",
            entity_key=year_key,
            source_checksum=blob_metadata.md5_hash,
            checksum_kind="gcs_md5",
            source_uris=(uri,),
            target_table_id=table_id,
        )
    return year_state


def load_worldbank_energy(
    *,
    source_mode: str,
    local_silver_root: Path,
    gcs_prefix: str | None,
    table_name: str,
    replace_touched_partitions: bool,
    include_loaded_years: bool,
    selected_years: set[str],
    since_year: str | None,
    until_year: str | None,
    dry_run: bool,
    log_level: str,
) -> dict[str, object]:
    config = GcpCloudConfig.from_env()
    resolved_gcs_prefix = gcs_prefix or config.blob_path(*DEFAULT_GCS_PREFIX_PARTS)
    table_id = f"{config.gcp_project_id}.{config.bq_raw_dataset}.{table_name}"
    audit_table_id = f"{config.gcp_project_id}.{config.bq_raw_dataset}.{AUDIT_TABLE_NAME}"
    state_table_id = f"{config.gcp_project_id}.{config.bq_raw_dataset}.{STATE_TABLE_NAME}"
    run_id = build_run_id("worldbank_energy_load_bigquery")
    logger = configure_logger(
        logger_name=LOGGER_NAME,
        log_path=LOG_PATH,
        log_level=log_level,
    )
    started_at = datetime.now(timezone.utc)

    manifest_entry: dict[str, object] = {
        "run_id": run_id,
        "asset_name": "load_worldbank_energy_to_bigquery",
        "dataset_name": "worldbank_energy",
        "status": "running",
        "started_at": started_at.isoformat(),
        "finished_at": None,
        "duration_seconds": None,
        "table_id": table_id,
        "source_mode": source_mode,
        "gcs_prefix": resolved_gcs_prefix,
        "replace_touched_partitions": replace_touched_partitions,
        "include_loaded_years": include_loaded_years,
        "selected_years_filter": sorted(selected_years),
        "since_year": since_year,
        "until_year": until_year,
        "candidate_years": [],
        "selected_years": [],
        "skipped_loaded_years": [],
        "candidate_file_count": 0,
        "selected_file_count": 0,
        "output_rows": None,
        "audit_table_id": audit_table_id,
        "state_table_id": state_table_id,
        "error_summary": None,
        "dry_run": dry_run,
        "skipped_unchanged_years": [],
    }
    client = None
    audit_table_ready = False

    try:
        logger.info("Starting World Bank energy BigQuery load run_id=%s", run_id)
        logger.info("Loading into %s from source=%s", table_id, source_mode)
        logger.info("Step 1/4 Discover candidate World Bank energy parquet partitions")

        if source_mode == "local":
            partition_files = _partition_files(local_silver_root)
            if not partition_files:
                raise FileNotFoundError(
                    f"No World Bank energy partition files found under {local_silver_root}. "
                    "Run the silver build before loading BigQuery, or use --source gcs."
                )
            candidate_uris = [
                _uri_from_resolved_prefix(
                    bucket_name=config.gcs_bucket,
                    resolved_prefix=resolved_gcs_prefix,
                    relative_path=path.relative_to(local_silver_root).as_posix(),
                )
                for path in partition_files
            ]
        else:
            candidate_uris = _gcs_partition_uris(config, resolved_gcs_prefix)
            if not candidate_uris:
                raise FileNotFoundError(
                    f"No World Bank energy parquet objects found under gs://{config.gcs_bucket}/{resolved_gcs_prefix}"
                )

        filtered_candidate_uris, candidate_years = _filter_candidate_uris(
            candidate_uris=candidate_uris,
            selected_years=selected_years,
            since_year=since_year,
            until_year=until_year,
        )
        if not filtered_candidate_uris:
            raise FileNotFoundError("No World Bank energy partition files matched the requested year filters.")

        candidate_year_text = [f"{year:04d}" for year in candidate_years]
        summary: dict[str, object] = {
            "run_id": run_id,
            "project_id": config.gcp_project_id,
            "location": config.gcp_location,
            "table_id": table_id,
            "source_mode": source_mode,
            "local_silver_root": str(local_silver_root),
            "gcs_prefix": resolved_gcs_prefix,
            "candidate_file_count": len(filtered_candidate_uris),
            "candidate_years": candidate_year_text,
            "replace_touched_partitions": replace_touched_partitions,
            "include_loaded_years": include_loaded_years,
            "selected_years_filter": sorted(selected_years),
            "since_year": since_year,
            "until_year": until_year,
            "dry_run": dry_run,
            "log_path": str(LOG_PATH),
            "manifest_path": str(MANIFEST_PATH),
            "audit_table_id": manifest_entry["audit_table_id"],
            "state_table_id": manifest_entry["state_table_id"],
        }
        manifest_entry["candidate_file_count"] = len(filtered_candidate_uris)
        manifest_entry["candidate_years"] = candidate_year_text
        year_state = _source_state_by_year(
            config=config,
            resolved_gcs_prefix=resolved_gcs_prefix,
            candidate_uris=filtered_candidate_uris,
            table_id=table_id,
        )
        logger.info(
            "Discovered candidate_files=%s candidate_years=%s",
            len(filtered_candidate_uris),
            candidate_year_text,
        )

        if dry_run:
            summary["status"] = "planned"
            summary["gcs_source_uris"] = filtered_candidate_uris[:20]
            finished_at = datetime.now(timezone.utc)
            summary["duration_seconds"] = duration_seconds(started_at, finished_at)
            manifest_entry.update(
                {
                    "status": "planned",
                    "finished_at": finished_at.isoformat(),
                }
            )
            manifest_entry["duration_seconds"] = duration_seconds(started_at, finished_at)
            append_manifest(MANIFEST_PATH, manifest_entry)
            logger.info("Dry-run complete for run_id=%s", run_id)
            return json_ready(summary)

        logger.info("Step 2/4 Resolve BigQuery dataset and selected years")
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
            entity_type="fact_partition",
            entity_keys=sorted(year_state.keys()),
            location=config.gcp_location,
        )
        selected_year_text, skipped_year_text = _selected_uris_by_year(
            year_state=year_state,
            include_loaded_years=include_loaded_years,
            state_checksums=state_checksums,
            client=client,
            table_exists=table_exists,
        )
        selected_uris = [year_state[year].source_uris[0] for year in selected_year_text]
        selected_year_values = [int(year) for year in selected_year_text]

        summary["selected_file_count"] = len(selected_uris)
        summary["selected_years"] = selected_year_text
        summary["skipped_loaded_years"] = skipped_year_text
        summary["skipped_unchanged_years"] = skipped_year_text
        summary["gcs_source_uris"] = selected_uris[:20]
        manifest_entry["selected_file_count"] = len(selected_uris)
        manifest_entry["selected_years"] = selected_year_text
        manifest_entry["skipped_loaded_years"] = skipped_year_text
        manifest_entry["skipped_unchanged_years"] = skipped_year_text

        logger.info(
            "Candidate years=%s selected_changed=%s skipped_unchanged=%s",
            candidate_year_text,
            selected_year_text,
            skipped_year_text,
        )

        if not selected_uris:
            summary["status"] = "no_op_all_candidate_years_already_loaded"
            finished_at = datetime.now(timezone.utc)
            summary["duration_seconds"] = duration_seconds(started_at, finished_at)
            manifest_entry.update(
                {
                    "status": summary["status"],
                    "finished_at": finished_at.isoformat(),
                }
            )
            manifest_entry["duration_seconds"] = duration_seconds(started_at, finished_at)
            audit_errors = _write_audit_row(client, audit_table_id=audit_table_id, entry=manifest_entry)
            if audit_errors:
                logger.warning("Audit row insert returned errors: %s", audit_errors)
            append_manifest(MANIFEST_PATH, manifest_entry)
            logger.info("No-op load complete for run_id=%s", run_id)
            return json_ready(summary)

        if table_exists and replace_touched_partitions:
            logger.info("Step 3/4 Delete touched year partitions from %s", table_id)
            delete_sql = f"""
            delete from `{table_id}`
            where year in unnest(@touched_years)
            """
            delete_job = client.query(
                delete_sql,
                location=config.gcp_location,
                job_config=bigquery.QueryJobConfig(
                    query_parameters=[
                        bigquery.ArrayQueryParameter(
                            "touched_years",
                            "INT64",
                            selected_year_values,
                        )
                    ]
                ),
            )
            delete_job.result()
            logger.info("Deleted existing rows for years=%s", selected_year_text)

        logger.info(
            "Step 4/4 Submit World Bank energy load job uris=%s years=%s",
            len(selected_uris),
            selected_year_text,
        )
        load_job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.PARQUET,
            create_disposition=bigquery.CreateDisposition.CREATE_IF_NEEDED,
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
            schema_update_options=[bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION],
            time_partitioning=bigquery.TimePartitioning(
                type_=bigquery.TimePartitioningType.DAY,
                field="month_start_date",
            ),
            clustering_fields=["indicator_code", "country_iso3"],
        )
        load_job = client.load_table_from_uri(
            selected_uris,
            table_id,
            location=config.gcp_location,
            job_config=load_job_config,
        )
        load_job.result()

        replace_load_state_rows(
            client,
            bigquery=bigquery,
            state_table_id=state_table_id,
            rows=[year_state[year] for year in selected_year_text],
            run_id=run_id,
            source_mode=source_mode,
            location=config.gcp_location,
        )

        table = client.get_table(table_id)
        finished_at = datetime.now(timezone.utc)
        summary["status"] = "loaded"
        summary["output_rows"] = table.num_rows
        summary["duration_seconds"] = duration_seconds(started_at, finished_at)
        manifest_entry.update(
            {
                "status": "loaded",
                "finished_at": finished_at.isoformat(),
                "duration_seconds": duration_seconds(started_at, finished_at),
                "output_rows": table.num_rows,
            }
        )
        audit_errors = _write_audit_row(client, audit_table_id=audit_table_id, entry=manifest_entry)
        if audit_errors:
            logger.warning("Audit row insert returned errors: %s", audit_errors)
        append_manifest(MANIFEST_PATH, manifest_entry)
        logger.info(
            "Finished World Bank energy BigQuery load run_id=%s output_rows=%s duration_s=%.3f",
            run_id,
            table.num_rows,
            manifest_entry["duration_seconds"],
        )
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
        logger.exception("World Bank energy BigQuery load failed run_id=%s", run_id)
        raise


def _build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Load World Bank energy silver parquet from GCS into BigQuery raw.energy_vulnerability."
    )
    parser.add_argument(
        "--source",
        choices=("gcs", "local"),
        default="gcs",
        help="Read candidate partition files from GCS or derive GCS URIs from a local silver tree.",
    )
    parser.add_argument(
        "--local-silver-root",
        default=str(DEFAULT_LOCAL_SILVER_ROOT),
        help="Local root for the canonical World Bank energy silver annual partitions when --source local is used.",
    )
    parser.add_argument(
        "--gcs-prefix",
        default=None,
        help="Optional blob prefix inside the configured bucket. Defaults to silver/worldbank_energy/energy_vulnerability.",
    )
    parser.add_argument("--table-name", default="energy_vulnerability", help="BigQuery table name inside the raw dataset.")
    parser.add_argument(
        "--append-only",
        action="store_true",
        help="When loading selected years, skip the delete step and append rows into the landing table.",
    )
    parser.add_argument(
        "--include-loaded-years",
        action="store_true",
        help="Reload years that are already present in BigQuery instead of skipping them.",
    )
    parser.add_argument(
        "--year",
        action="append",
        default=None,
        help="Restrict the load to a specific year. Repeat for multiple years.",
    )
    parser.add_argument("--since-year", default=None, help="Restrict loads to years from YYYY onward.")
    parser.add_argument("--until-year", default=None, help="Restrict loads to years through YYYY.")
    parser.add_argument("--log-level", default="INFO", help="Logging level, for example INFO or DEBUG.")
    parser.add_argument("--dry-run", action="store_true", help="Show the intended load job without calling BigQuery.")
    return parser


def main() -> None:
    parser = _build_arg_parser()
    args = parser.parse_args()

    selected_years = {_parse_year(value) for value in (args.year or [])}
    since_year = _parse_year(args.since_year) if args.since_year else None
    until_year = _parse_year(args.until_year) if args.until_year else None

    summary = load_worldbank_energy(
        source_mode=args.source,
        local_silver_root=Path(args.local_silver_root),
        gcs_prefix=args.gcs_prefix,
        table_name=args.table_name,
        replace_touched_partitions=not args.append_only,
        include_loaded_years=args.include_loaded_years,
        selected_years=selected_years,
        since_year=since_year,
        until_year=until_year,
        dry_run=args.dry_run,
        log_level=args.log_level,
    )
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
