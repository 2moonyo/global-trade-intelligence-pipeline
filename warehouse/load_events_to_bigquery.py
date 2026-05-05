from __future__ import annotations

import argparse
import json
import sys
from dataclasses import dataclass
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
    composite_checksum,
    ensure_load_state_table,
    fetch_load_state_checksums,
    replace_load_state_rows,
)


DEFAULT_LOCAL_DIM_EVENT_PATH = PROJECT_ROOT / "data" / "silver" / "events" / "dim_event.parquet"
DEFAULT_LOCAL_CORE_BRIDGE_ROOT = PROJECT_ROOT / "data" / "silver" / "events" / "bridge_event_month_chokepoint_core"
DEFAULT_LOCAL_REGION_BRIDGE_ROOT = PROJECT_ROOT / "data" / "silver" / "events" / "bridge_event_month_maritime_region"
DEFAULT_DIM_EVENT_GCS_PREFIX = ("silver", "events", "dim_event.parquet")
DEFAULT_CORE_BRIDGE_GCS_PREFIX = ("silver", "events", "bridge_event_month_chokepoint_core")
DEFAULT_REGION_BRIDGE_GCS_PREFIX = ("silver", "events", "bridge_event_month_maritime_region")
PARQUET_FILENAME = ".parquet"
LOGGER_NAME = "events.load_bigquery"
LOG_DIR = PROJECT_ROOT / "logs" / "events"
LOG_PATH = LOG_DIR / "load_events_to_bigquery.log"
MANIFEST_PATH = LOG_DIR / "load_events_to_bigquery_manifest.jsonl"
AUDIT_TABLE_NAME = "events_load_audit"
STATE_TABLE_NAME = "events_load_state"


def _bigquery_imports():
    try:
        from google.api_core.exceptions import NotFound
        from google.cloud import bigquery
    except ModuleNotFoundError as exc:
        raise ModuleNotFoundError(
            "google-cloud-bigquery is required for events BigQuery load operations."
        ) from exc
    return bigquery, NotFound


def _uri_from_resolved_prefix(*, bucket_name: str, resolved_prefix: str, relative_path: str | None = None) -> str:
    prefix = resolved_prefix.strip("/")
    if relative_path:
        return f"gs://{bucket_name}/{prefix}/{relative_path.lstrip('/')}"
    return f"gs://{bucket_name}/{prefix}"


def _path_year_month(path: Path, root: Path) -> str | None:
    for part in path.relative_to(root).parts:
        if part.startswith("year_month="):
            return part.split("=", 1)[1]
    return None


def _gcs_year_month(uri: str) -> str | None:
    parsed = urlparse(uri)
    for part in parsed.path.strip("/").split("/"):
        if part.startswith("year_month="):
            return part.split("=", 1)[1]
    return None


def _parse_year_month(value: str) -> str:
    from datetime import date

    parsed = date.fromisoformat(f"{value}-01")
    return parsed.strftime("%Y-%m")


def _matches_year_month_filters(
    *,
    year_month: str | None,
    selected_year_months: set[str],
    since_year_month: str | None,
    until_year_month: str | None,
) -> bool:
    if year_month is None:
        return True
    if selected_year_months and year_month not in selected_year_months:
        return False
    if since_year_month and year_month < since_year_month:
        return False
    if until_year_month and year_month > until_year_month:
        return False
    return True


@dataclass(frozen=True)
class EventLoadSpec:
    name: str
    table_name: str
    entity_type: str
    local_mode: str
    local_path: Path
    gcs_prefix: tuple[str, ...]
    clustering_fields: tuple[str, ...]
    supports_year_month_filters: bool


EVENT_LOAD_SPECS = [
    EventLoadSpec(
        name="dim_event",
        table_name="dim_event",
        entity_type="table_asset",
        local_mode="file",
        local_path=DEFAULT_LOCAL_DIM_EVENT_PATH,
        gcs_prefix=DEFAULT_DIM_EVENT_GCS_PREFIX,
        clustering_fields=("event_id", "event_type"),
        supports_year_month_filters=False,
    ),
    EventLoadSpec(
        name="bridge_event_month_chokepoint_core",
        table_name="bridge_event_month_chokepoint_core",
        entity_type="table_asset",
        local_mode="directory",
        local_path=DEFAULT_LOCAL_CORE_BRIDGE_ROOT,
        gcs_prefix=DEFAULT_CORE_BRIDGE_GCS_PREFIX,
        clustering_fields=("event_id", "year_month"),
        supports_year_month_filters=True,
    ),
    EventLoadSpec(
        name="bridge_event_month_maritime_region",
        table_name="bridge_event_month_maritime_region",
        entity_type="table_asset",
        local_mode="directory",
        local_path=DEFAULT_LOCAL_REGION_BRIDGE_ROOT,
        gcs_prefix=DEFAULT_REGION_BRIDGE_GCS_PREFIX,
        clustering_fields=("event_id", "year_month"),
        supports_year_month_filters=True,
    ),
]


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
        bigquery.SchemaField("source_mode", "STRING"),
        bigquery.SchemaField("selected_assets", "STRING", mode="REPEATED"),
        bigquery.SchemaField("selected_year_months", "STRING", mode="REPEATED"),
        bigquery.SchemaField("skipped_unchanged_assets", "STRING", mode="REPEATED"),
        bigquery.SchemaField("loaded_assets", "STRING", mode="REPEATED"),
        bigquery.SchemaField("asset_results_json", "STRING"),
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
        "source_mode": entry.get("source_mode"),
        "selected_assets": entry.get("selected_assets", []),
        "selected_year_months": entry.get("selected_year_months", []),
        "skipped_unchanged_assets": entry.get("skipped_unchanged_assets", []),
        "loaded_assets": entry.get("loaded_assets", []),
        "asset_results_json": json.dumps(entry.get("asset_results", {})),
        "error_summary": entry.get("error_summary"),
    }
    return client.insert_rows_json(audit_table_id, [row])


def _candidate_uris_for_spec(
    *,
    config: GcpCloudConfig,
    spec: EventLoadSpec,
    source_mode: str,
    selected_year_months: set[str],
    since_year_month: str | None,
    until_year_month: str | None,
) -> tuple[str, list[str], list[str]]:
    resolved_gcs_prefix = config.blob_path(*spec.gcs_prefix)

    if source_mode == "local":
        if spec.local_mode == "file":
            if not spec.local_path.exists():
                raise FileNotFoundError(f"Missing local events silver file: {spec.local_path}")
            candidate_uris = [
                _uri_from_resolved_prefix(
                    bucket_name=config.gcs_bucket,
                    resolved_prefix=resolved_gcs_prefix,
                )
            ]
            return resolved_gcs_prefix, candidate_uris, []

        if not spec.local_path.exists():
            raise FileNotFoundError(f"Missing local events silver directory: {spec.local_path}")
        local_files = sorted(path for path in spec.local_path.rglob("*.parquet") if path.is_file())
        filtered_files = [
            path
            for path in local_files
            if _matches_year_month_filters(
                year_month=_path_year_month(path, spec.local_path),
                selected_year_months=selected_year_months,
                since_year_month=since_year_month,
                until_year_month=until_year_month,
            )
        ]
        candidate_uris = [
            _uri_from_resolved_prefix(
                bucket_name=config.gcs_bucket,
                resolved_prefix=resolved_gcs_prefix,
                relative_path=path.relative_to(spec.local_path).as_posix(),
            )
            for path in filtered_files
        ]
        touched = sorted(
            {
                value
                for path in filtered_files
                if (value := _path_year_month(path, spec.local_path)) is not None
            }
        )
        return resolved_gcs_prefix, candidate_uris, touched

    if spec.local_mode == "file":
        candidate_uris = list_blob_uris(
            bucket_name=config.gcs_bucket,
            prefix=resolved_gcs_prefix,
            project_id=config.gcp_project_id,
            suffix=".parquet",
        )
        return resolved_gcs_prefix, candidate_uris, []

    candidate_uris = list_blob_uris(
        bucket_name=config.gcs_bucket,
        prefix=resolved_gcs_prefix,
        project_id=config.gcp_project_id,
        suffix=".parquet",
    )
    filtered_uris = [
        uri
        for uri in candidate_uris
        if _matches_year_month_filters(
            year_month=_gcs_year_month(uri),
            selected_year_months=selected_year_months,
            since_year_month=since_year_month,
            until_year_month=until_year_month,
        )
    ]
    touched = sorted({value for uri in filtered_uris if (value := _gcs_year_month(uri)) is not None})
    return resolved_gcs_prefix, filtered_uris, touched


def _source_state_for_spec(
    *,
    config: GcpCloudConfig,
    resolved_gcs_prefix: str,
    spec: EventLoadSpec,
    candidate_uris: list[str],
    table_id: str,
) -> LoadStateRecord:
    metadata_by_name = list_blob_metadata(
        bucket_name=config.gcs_bucket,
        prefix=resolved_gcs_prefix,
        project_id=config.gcp_project_id,
        suffix=".parquet",
    )
    entries: list[tuple[str, str]] = []
    for uri in candidate_uris:
        blob_name = blob_name_from_gcs_uri(uri)
        blob_metadata = metadata_by_name.get(blob_name)
        if blob_metadata is None:
            raise FileNotFoundError(f"Expected GCS object for events load source {uri}")
        if not blob_metadata.md5_hash:
            raise ValueError(f"GCS object {uri} is missing an md5 checksum; cannot perform checksum-aware load.")
        entries.append((uri, blob_metadata.md5_hash))

    if not entries:
        raise FileNotFoundError(f"No parquet objects found for events asset {spec.name} under {resolved_gcs_prefix}")

    checksum_kind = "gcs_md5" if len(entries) == 1 else "gcs_md5_composite_sha256"
    source_checksum = entries[0][1] if len(entries) == 1 else composite_checksum(entries)
    return LoadStateRecord(
        entity_type=spec.entity_type,
        entity_key=spec.name,
        source_checksum=source_checksum,
        checksum_kind=checksum_kind,
        source_uris=tuple(sorted(uri for uri, _ in entries)),
        target_table_id=table_id,
    )


def load_events(
    *,
    source_mode: str,
    include_dim_event: bool,
    include_core_bridge: bool,
    include_region_bridge: bool,
    include_loaded_assets: bool,
    selected_year_months: set[str],
    since_year_month: str | None,
    until_year_month: str | None,
    dry_run: bool,
    log_level: str,
) -> dict[str, object]:
    selected_specs = [
        spec
        for spec in EVENT_LOAD_SPECS
        if (spec.name == "dim_event" and include_dim_event)
        or (spec.name == "bridge_event_month_chokepoint_core" and include_core_bridge)
        or (spec.name == "bridge_event_month_maritime_region" and include_region_bridge)
    ]
    if not selected_specs:
        raise ValueError("Choose at least one events asset to load.")

    config = GcpCloudConfig.from_env()
    run_id = build_run_id("events_load_bigquery")
    logger = configure_logger(
        logger_name=LOGGER_NAME,
        log_path=LOG_PATH,
        log_level=log_level,
    )
    started_at = datetime.now(timezone.utc)

    summary: dict[str, object] = {
        "run_id": run_id,
        "project_id": config.gcp_project_id,
        "location": config.gcp_location,
        "source_mode": source_mode,
        "selected_assets": [spec.name for spec in selected_specs],
        "selected_year_months": sorted(selected_year_months),
        "since_year_month": since_year_month,
        "until_year_month": until_year_month,
        "include_loaded_assets": include_loaded_assets,
        "dry_run": dry_run,
        "log_path": str(LOG_PATH),
        "manifest_path": str(MANIFEST_PATH),
        "asset_results": {},
    }
    manifest_entry: dict[str, object] = {
        "run_id": run_id,
        "asset_name": "load_events_to_bigquery",
        "dataset_name": "events",
        "status": "running",
        "started_at": started_at.isoformat(),
        "finished_at": None,
        "duration_seconds": None,
        "source_mode": source_mode,
        "selected_assets": [spec.name for spec in selected_specs],
        "selected_year_months": sorted(selected_year_months),
        "since_year_month": since_year_month,
        "until_year_month": until_year_month,
        "include_loaded_assets": include_loaded_assets,
        "asset_results": {},
        "loaded_assets": [],
        "skipped_unchanged_assets": [],
        "error_summary": None,
        "dry_run": dry_run,
    }
    client = None
    audit_table_ready = False

    try:
        logger.info("Starting events BigQuery load run_id=%s", run_id)
        asset_candidates: dict[str, dict[str, object]] = {}
        for spec in selected_specs:
            resolved_gcs_prefix, candidate_uris, touched_months = _candidate_uris_for_spec(
                config=config,
                spec=spec,
                source_mode=source_mode,
                selected_year_months=selected_year_months if spec.supports_year_month_filters else set(),
                since_year_month=since_year_month if spec.supports_year_month_filters else None,
                until_year_month=until_year_month if spec.supports_year_month_filters else None,
            )
            if not candidate_uris:
                raise FileNotFoundError(f"No candidate parquet objects found for events asset {spec.name}")
            asset_candidates[spec.name] = {
                "spec": spec,
                "resolved_gcs_prefix": resolved_gcs_prefix,
                "candidate_uris": candidate_uris,
                "touched_year_months": touched_months,
            }
            logger.info(
                "Discovered asset=%s candidate_files=%s touched_months=%s",
                spec.name,
                len(candidate_uris),
                touched_months,
            )

        if dry_run:
            summary["status"] = "planned"
            summary["asset_results"] = {
                name: {
                    "table_name": candidate["spec"].table_name,
                    "gcs_prefix": candidate["resolved_gcs_prefix"],
                    "candidate_file_count": len(candidate["candidate_uris"]),
                    "touched_year_months": candidate["touched_year_months"],
                    "sample_uris": candidate["candidate_uris"][:20],
                }
                for name, candidate in asset_candidates.items()
            }
            finished_at = datetime.now(timezone.utc)
            summary["duration_seconds"] = duration_seconds(started_at, finished_at)
            manifest_entry.update(
                {
                    "status": "planned",
                    "finished_at": finished_at.isoformat(),
                    "duration_seconds": duration_seconds(started_at, finished_at),
                    "asset_results": summary["asset_results"],
                }
            )
            append_manifest(MANIFEST_PATH, manifest_entry)
            logger.info("Dry-run complete for run_id=%s", run_id)
            return json_ready(summary)

        bigquery, _ = _bigquery_imports()
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
        state_table_id = ensure_load_state_table(
            client,
            bigquery=bigquery,
            project_id=config.gcp_project_id,
            dataset_name=config.bq_raw_dataset,
            table_name=STATE_TABLE_NAME,
        )

        state_checksums = fetch_load_state_checksums(
            client,
            bigquery=bigquery,
            state_table_id=state_table_id,
            entity_type="table_asset",
            entity_keys=sorted(asset_candidates.keys()),
            location=config.gcp_location,
        )

        loaded_state_rows: list[LoadStateRecord] = []
        asset_results: dict[str, object] = {}
        loaded_assets: list[str] = []
        skipped_assets: list[str] = []

        for name, candidate in asset_candidates.items():
            spec: EventLoadSpec = candidate["spec"]
            table_id = f"{config.gcp_project_id}.{config.bq_raw_dataset}.{spec.table_name}"
            state_row = _source_state_for_spec(
                config=config,
                resolved_gcs_prefix=candidate["resolved_gcs_prefix"],
                spec=spec,
                candidate_uris=candidate["candidate_uris"],
                table_id=table_id,
            )

            result_entry = {
                "table_id": table_id,
                "gcs_prefix": candidate["resolved_gcs_prefix"],
                "candidate_file_count": len(candidate["candidate_uris"]),
                "touched_year_months": candidate["touched_year_months"],
                "source_uri_count": len(state_row.source_uris),
                "sample_uris": list(state_row.source_uris[:20]),
                "status": "pending",
            }

            prior_checksum = state_checksums.get(spec.name)
            if not include_loaded_assets and prior_checksum is not None and prior_checksum == state_row.source_checksum:
                result_entry["status"] = "skipped_unchanged"
                skipped_assets.append(spec.name)
                asset_results[spec.name] = result_entry
                logger.info("Skipping unchanged events asset=%s table=%s", spec.name, table_id)
                continue

            logger.info(
                "Loading events asset=%s into %s files=%s",
                spec.name,
                table_id,
                len(state_row.source_uris),
            )
            load_job = client.load_table_from_uri(
                list(state_row.source_uris),
                table_id,
                location=config.gcp_location,
                job_config=bigquery.LoadJobConfig(
                    source_format=bigquery.SourceFormat.PARQUET,
                    create_disposition=bigquery.CreateDisposition.CREATE_IF_NEEDED,
                    write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
                    clustering_fields=list(spec.clustering_fields),
                ),
            )
            load_job.result()
            table = client.get_table(table_id)
            loaded_state_rows.append(state_row)
            loaded_assets.append(spec.name)
            result_entry["status"] = "loaded"
            result_entry["output_rows"] = table.num_rows
            asset_results[spec.name] = result_entry

        replace_load_state_rows(
            client,
            bigquery=bigquery,
            state_table_id=state_table_id,
            rows=loaded_state_rows,
            run_id=run_id,
            source_mode=source_mode,
            location=config.gcp_location,
        )

        finished_at = datetime.now(timezone.utc)
        summary["status"] = "loaded" if loaded_assets else "no_op_all_selected_assets_already_loaded"
        summary["duration_seconds"] = duration_seconds(started_at, finished_at)
        summary["asset_results"] = asset_results
        summary["loaded_assets"] = loaded_assets
        summary["skipped_unchanged_assets"] = skipped_assets
        manifest_entry.update(
            {
                "status": summary["status"],
                "finished_at": finished_at.isoformat(),
                "duration_seconds": duration_seconds(started_at, finished_at),
                "asset_results": asset_results,
                "loaded_assets": loaded_assets,
                "skipped_unchanged_assets": skipped_assets,
            }
        )
        audit_errors = _write_audit_row(client, audit_table_id=audit_table_id, entry=manifest_entry)
        if audit_errors:
            logger.warning("Audit row insert returned errors: %s", audit_errors)
        append_manifest(MANIFEST_PATH, manifest_entry)
        logger.info(
            "Finished events BigQuery load run_id=%s loaded_assets=%s skipped_assets=%s duration_s=%.3f",
            run_id,
            loaded_assets,
            skipped_assets,
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
                logger.exception("Failed to write events BigQuery audit row during failure handling run_id=%s", run_id)
        append_manifest(MANIFEST_PATH, manifest_entry)
        logger.exception("Events BigQuery load failed run_id=%s", run_id)
        raise


def _build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Load events silver parquet from GCS into BigQuery raw.dim_event/raw.bridge_event_*."
    )
    parser.add_argument(
        "--source",
        choices=("gcs", "local"),
        default="gcs",
        help="Read candidate files from GCS or derive GCS URIs from a local silver tree.",
    )
    parser.add_argument("--skip-dim-event", action="store_true", help="Do not load raw.dim_event.")
    parser.add_argument(
        "--skip-core-bridge",
        action="store_true",
        help="Do not load raw.bridge_event_month_chokepoint_core.",
    )
    parser.add_argument(
        "--skip-region-bridge",
        action="store_true",
        help="Do not load raw.bridge_event_month_maritime_region.",
    )
    parser.add_argument(
        "--include-loaded-assets",
        action="store_true",
        help="Reload selected assets even when their source checksums match the previous successful load.",
    )
    parser.add_argument(
        "--year-month",
        action="append",
        default=None,
        help="Restrict candidate bridge files to a specific YYYY-MM value when discovering sources.",
    )
    parser.add_argument("--since-year-month", default=None, help="Restrict candidate bridge files to months from YYYY-MM onward.")
    parser.add_argument("--until-year-month", default=None, help="Restrict candidate bridge files to months through YYYY-MM.")
    parser.add_argument("--log-level", default="INFO", help="Logging level, for example INFO or DEBUG.")
    parser.add_argument("--dry-run", action="store_true", help="Show planned loads without calling BigQuery.")
    return parser


def main() -> None:
    parser = _build_arg_parser()
    args = parser.parse_args()

    if args.skip_dim_event and args.skip_core_bridge and args.skip_region_bridge:
        parser.error("Choose at least one of dim_event, core_bridge, or region_bridge to load.")

    selected_year_months = {_parse_year_month(value) for value in (args.year_month or [])}
    since_year_month = _parse_year_month(args.since_year_month) if args.since_year_month else None
    until_year_month = _parse_year_month(args.until_year_month) if args.until_year_month else None

    summary = load_events(
        source_mode=args.source,
        include_dim_event=not args.skip_dim_event,
        include_core_bridge=not args.skip_core_bridge,
        include_region_bridge=not args.skip_region_bridge,
        include_loaded_assets=args.include_loaded_assets,
        selected_year_months=selected_year_months,
        since_year_month=since_year_month,
        until_year_month=until_year_month,
        dry_run=args.dry_run,
        log_level=args.log_level,
    )
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
