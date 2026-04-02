from __future__ import annotations

import argparse
import json
import sys
from datetime import date, datetime, timezone
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from ingest.common.cloud_config import GcpCloudConfig
from ingest.common.run_artifacts import (
    append_manifest,
    build_run_id,
    configure_logger,
    duration_seconds,
    json_ready,
)
from warehouse.gcs_publish_common import UploadSpec, publish_directory_spec, publish_file_spec


LOGGER_NAME = "events.publish_gcs"
LOG_DIR = PROJECT_ROOT / "logs" / "events"
LOG_PATH = LOG_DIR / "publish_events_to_gcs.log"
MANIFEST_PATH = LOG_DIR / "publish_events_to_gcs_manifest.jsonl"


def _parse_year_month(value: str) -> str:
    parsed = date.fromisoformat(f"{value}-01")
    return parsed.strftime("%Y-%m")


def _path_year_month(path: Path, root: Path) -> str | None:
    for part in path.relative_to(root).parts:
        if part.startswith("year_month="):
            return part.split("=", 1)[1]
    return None


def _build_specs(
    *,
    include_dim_event: bool,
    include_core_bridge: bool,
    include_region_bridge: bool,
) -> tuple[list[UploadSpec], list[UploadSpec]]:
    file_specs: list[UploadSpec] = []
    directory_specs: list[UploadSpec] = []

    if include_dim_event:
        file_specs.append(
            UploadSpec(
                name="silver_events_dim_event",
                local_path=PROJECT_ROOT / "data" / "silver" / "events" / "dim_event.parquet",
                destination_parts=("silver", "events", "dim_event.parquet"),
            )
        )

    if include_core_bridge:
        directory_specs.append(
            UploadSpec(
                name="silver_events_core_bridge",
                local_path=PROJECT_ROOT / "data" / "silver" / "events" / "bridge_event_month_chokepoint_core",
                destination_parts=("silver", "events", "bridge_event_month_chokepoint_core"),
                include_suffixes=(".parquet",),
                partition_value_resolver=_path_year_month,
            )
        )

    if include_region_bridge:
        directory_specs.append(
            UploadSpec(
                name="silver_events_region_bridge",
                local_path=PROJECT_ROOT / "data" / "silver" / "events" / "bridge_event_month_maritime_region",
                destination_parts=("silver", "events", "bridge_event_month_maritime_region"),
                include_suffixes=(".parquet",),
                partition_value_resolver=_path_year_month,
            )
        )

    return file_specs, directory_specs


def publish_events_assets(
    *,
    include_dim_event: bool,
    include_core_bridge: bool,
    include_region_bridge: bool,
    skip_existing: bool,
    selected_year_months: set[str],
    since_year_month: str | None,
    until_year_month: str | None,
    dry_run: bool,
    log_level: str,
) -> dict[str, object]:
    config = GcpCloudConfig.from_env()
    file_specs, directory_specs = _build_specs(
        include_dim_event=include_dim_event,
        include_core_bridge=include_core_bridge,
        include_region_bridge=include_region_bridge,
    )
    run_id = build_run_id("events_publish_gcs")
    logger = configure_logger(
        logger_name=LOGGER_NAME,
        log_path=LOG_PATH,
        log_level=log_level,
    )
    started_at = datetime.now(timezone.utc)

    summary: dict[str, object] = {
        "run_id": run_id,
        "project_id": config.gcp_project_id,
        "bucket": config.gcs_bucket,
        "gcs_prefix": config.gcs_prefix,
        "skip_existing": skip_existing,
        "selected_year_months": sorted(selected_year_months),
        "since_year_month": since_year_month,
        "until_year_month": until_year_month,
        "dry_run": dry_run,
        "uploads": {},
        "log_path": str(LOG_PATH),
        "manifest_path": str(MANIFEST_PATH),
    }
    manifest_entry: dict[str, object] = {
        "run_id": run_id,
        "asset_name": "publish_events_to_gcs",
        "dataset_name": "events",
        "status": "running",
        "started_at": started_at.isoformat(),
        "finished_at": None,
        "duration_seconds": None,
        "skip_existing": skip_existing,
        "selected_year_months": sorted(selected_year_months),
        "since_year_month": since_year_month,
        "until_year_month": until_year_month,
        "dry_run": dry_run,
        "touched_year_months": [],
        "uploads": {},
        "error_summary": None,
    }

    try:
        logger.info("Starting events GCS publish run_id=%s", run_id)
        logger.info("Publishing into gs://%s/%s", config.gcs_bucket, config.gcs_prefix)
        uploads: dict[str, object] = {}
        touched_year_months: set[str] = set()

        for spec in file_specs:
            if not spec.local_path.exists():
                raise FileNotFoundError(f"Missing required events source file: {spec.local_path}")
            upload_summary = publish_file_spec(
                spec=spec,
                config=config,
                skip_existing=skip_existing,
                dry_run=dry_run,
            )
            uploads[spec.name] = {**upload_summary, "touched_year_months": []}
            logger.info(
                "Spec=%s action=%s checksum_match=%s upload_reason=%s uri=%s",
                spec.name,
                upload_summary["status"],
                upload_summary["checksum_match"],
                upload_summary["upload_reason"],
                upload_summary["gcs_destination"],
            )

        for spec in directory_specs:
            if not spec.local_path.exists():
                raise FileNotFoundError(f"Missing required events source directory: {spec.local_path}")
            upload_summary, _, spec_touched_months = publish_directory_spec(
                spec=spec,
                config=config,
                skip_existing=skip_existing,
                dry_run=dry_run,
                selected_partition_values=selected_year_months,
                since_partition_value=since_year_month,
                until_partition_value=until_year_month,
                logger=logger,
            )
            touched_year_months.update(spec_touched_months)
            uploads[spec.name] = {**upload_summary, "touched_year_months": spec_touched_months}
            logger.info(
                "Spec=%s considered=%s uploaded=%s skipped_existing=%s checksum_matched=%s checksum_mismatched=%s touched_months=%s",
                spec.name,
                upload_summary["files_considered"],
                upload_summary["files_uploaded"],
                upload_summary["files_skipped_existing"],
                upload_summary["files_checksum_matched"],
                upload_summary["files_checksum_mismatched"],
                spec_touched_months,
            )

        finished_at = datetime.now(timezone.utc)
        summary["uploads"] = uploads
        summary["touched_year_months"] = sorted(touched_year_months)
        summary["status"] = "completed" if not dry_run else "planned"
        summary["duration_seconds"] = duration_seconds(started_at, finished_at)
        manifest_entry.update(
            {
                "status": summary["status"],
                "finished_at": finished_at.isoformat(),
                "duration_seconds": duration_seconds(started_at, finished_at),
                "touched_year_months": sorted(touched_year_months),
                "uploads": uploads,
            }
        )
        append_manifest(MANIFEST_PATH, manifest_entry)
        logger.info(
            "Finished events GCS publish run_id=%s touched_year_months=%s duration_s=%.3f",
            run_id,
            len(touched_year_months),
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
        append_manifest(MANIFEST_PATH, manifest_entry)
        logger.exception("Events GCS publish failed run_id=%s", run_id)
        raise


def _build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Publish the events silver slice from local storage to GCS."
    )
    parser.add_argument("--skip-dim-event", action="store_true", help="Do not publish dim_event.parquet.")
    parser.add_argument(
        "--skip-core-bridge",
        action="store_true",
        help="Do not publish the core chokepoint event bridge parquet partitions.",
    )
    parser.add_argument(
        "--skip-region-bridge",
        action="store_true",
        help="Do not publish the regional event bridge parquet partitions.",
    )
    parser.add_argument(
        "--overwrite-existing",
        action="store_true",
        help="Re-upload files even when the destination blob already exists.",
    )
    parser.add_argument(
        "--year-month",
        action="append",
        default=None,
        help="Restrict partitioned bridge uploads to a specific YYYY-MM value. Repeat for multiple months.",
    )
    parser.add_argument(
        "--since-year-month",
        default=None,
        help="Restrict partitioned bridge uploads to months from YYYY-MM onward.",
    )
    parser.add_argument(
        "--until-year-month",
        default=None,
        help="Restrict partitioned bridge uploads to months through YYYY-MM.",
    )
    parser.add_argument("--log-level", default="INFO", help="Logging level, for example INFO or DEBUG.")
    parser.add_argument("--dry-run", action="store_true", help="Show planned uploads without calling GCS.")
    return parser


def main() -> None:
    parser = _build_arg_parser()
    args = parser.parse_args()

    if args.skip_dim_event and args.skip_core_bridge and args.skip_region_bridge:
        parser.error("Choose at least one of dim_event, core_bridge, or region_bridge to publish.")

    selected_year_months = {_parse_year_month(value) for value in (args.year_month or [])}
    since_year_month = _parse_year_month(args.since_year_month) if args.since_year_month else None
    until_year_month = _parse_year_month(args.until_year_month) if args.until_year_month else None

    summary = publish_events_assets(
        include_dim_event=not args.skip_dim_event,
        include_core_bridge=not args.skip_core_bridge,
        include_region_bridge=not args.skip_region_bridge,
        skip_existing=not args.overwrite_existing,
        selected_year_months=selected_year_months,
        since_year_month=since_year_month,
        until_year_month=until_year_month,
        dry_run=args.dry_run,
        log_level=args.log_level,
    )
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
