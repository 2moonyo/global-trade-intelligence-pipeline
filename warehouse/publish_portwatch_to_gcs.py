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
from warehouse.gcs_publish_common import (
    UploadSpec,
    path_year_month,
    publish_directory_spec,
    publish_file_spec,
)


LOGGER_NAME = "portwatch.publish_gcs"
LOG_DIR = PROJECT_ROOT / "logs" / "portwatch"
LOG_PATH = LOG_DIR / "publish_portwatch_to_gcs.log"
MANIFEST_PATH = LOG_DIR / "publish_portwatch_to_gcs_manifest.jsonl"


def _parse_year_month(value: str) -> str:
    parsed = date.fromisoformat(f"{value}-01")
    return parsed.strftime("%Y-%m")


def _build_specs(include_bronze: bool, include_silver: bool, include_auxiliary: bool) -> list[UploadSpec]:
    specs: list[UploadSpec] = [
        UploadSpec(
            name="metadata_portwatch",
            local_path=PROJECT_ROOT / "data" / "metadata" / "portwatch",
            destination_parts=("metadata", "portwatch"),
            include_suffixes=(".json",),
            optional=True,
        ),
    ]

    if include_bronze:
        specs.append(
            UploadSpec(
                name="bronze_portwatch",
                local_path=PROJECT_ROOT / "data" / "bronze" / "portwatch",
                destination_parts=("bronze", "portwatch"),
                include_suffixes=(".parquet",),
                partition_value_resolver=path_year_month,
            )
        )

    if include_silver:
        specs.append(
            UploadSpec(
                name="silver_portwatch_daily",
                local_path=PROJECT_ROOT / "data" / "silver" / "portwatch" / "portwatch_daily",
                destination_parts=("silver", "portwatch", "portwatch_daily"),
                include_suffixes=(".parquet",),
                partition_value_resolver=path_year_month,
            )
        )
        specs.append(
            UploadSpec(
                name="silver_portwatch_monthly",
                local_path=PROJECT_ROOT / "data" / "silver" / "portwatch" / "portwatch_monthly",
                destination_parts=("silver", "portwatch", "portwatch_monthly"),
                include_suffixes=(".parquet",),
                partition_value_resolver=path_year_month,
            )
        )

    if include_auxiliary:
        specs.extend(
            [
                UploadSpec(
                    name="silver_portwatch_dimensions",
                    local_path=PROJECT_ROOT / "data" / "silver" / "portwatch" / "dimensions",
                    destination_parts=("silver", "portwatch", "dimensions"),
                    include_suffixes=(".parquet",),
                    optional=True,
                ),
                UploadSpec(
                    name="silver_portwatch_scaffold",
                    local_path=PROJECT_ROOT
                    / "data"
                    / "silver"
                    / "portwatch"
                    / "portwatch_month_chokepoint_scaffold.parquet",
                    destination_parts=(
                        "silver",
                        "portwatch",
                        "portwatch_month_chokepoint_scaffold.parquet",
                    ),
                    optional=True,
                ),
            ]
        )

    return specs

def publish_portwatch_assets(
    *,
    include_bronze: bool,
    include_silver: bool,
    include_auxiliary: bool,
    skip_existing: bool,
    selected_year_months: set[str],
    since_year_month: str | None,
    until_year_month: str | None,
    dry_run: bool,
    log_level: str,
) -> dict[str, object]:
    config = GcpCloudConfig.from_env()
    specs = _build_specs(include_bronze=include_bronze, include_silver=include_silver, include_auxiliary=include_auxiliary)
    run_id = build_run_id("portwatch_publish_gcs")
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
        "asset_name": "publish_portwatch_to_gcs",
        "dataset_name": "portwatch",
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
        logger.info("Starting PortWatch GCS publish run_id=%s", run_id)
        logger.info("Publishing into gs://%s/%s", config.gcs_bucket, config.gcs_prefix)
        touched_year_months: set[str] = set()
        uploads: dict[str, object] = {}

        for spec in specs:
            if not spec.local_path.exists():
                if spec.optional:
                    uploads[spec.name] = {
                        "status": "skipped_missing_optional_source",
                        "local_path": str(spec.local_path),
                        "gcs_destination": config.gcs_uri(*spec.destination_parts),
                        "files_uploaded": 0,
                        "files_considered": 0,
                        "files_skipped_existing": 0,
                        "files_planned": 0,
                        "checksum_aware": True,
                        "checksum_verified_file_count": 0,
                        "files_checksum_matched": 0,
                        "files_checksum_mismatched": 0,
                        "files_checksum_unverified": 0,
                        "files_uploaded_missing_remote": 0,
                        "files_uploaded_checksum_mismatch": 0,
                        "files_uploaded_remote_checksum_unavailable": 0,
                        "all_compared_checksums_match": None,
                        "touched_year_months": [],
                        "sample_uris": [],
                        "sample_results": [],
                    }
                    continue
                if spec.name == "bronze_portwatch":
                    raise FileNotFoundError(
                        f"Missing required PortWatch source path: {spec.local_path}. "
                        "If you only need the silver slice, rerun with --skip-bronze."
                    )
                raise FileNotFoundError(f"Missing required PortWatch source path: {spec.local_path}")

            if spec.local_path.is_dir():
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
                uploads[spec.name] = {
                    **upload_summary,
                    "touched_year_months": spec_touched_months,
                }
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
                continue

            upload_summary = publish_file_spec(
                spec=spec,
                config=config,
                skip_existing=skip_existing,
                dry_run=dry_run,
            )
            uploads[spec.name] = {
                **upload_summary,
                "touched_year_months": [],
            }
            logger.info(
                "Spec=%s action=%s checksum_match=%s upload_reason=%s uri=%s",
                spec.name,
                upload_summary["status"],
                upload_summary["checksum_match"],
                upload_summary["upload_reason"],
                upload_summary["gcs_destination"],
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
            "Finished PortWatch GCS publish run_id=%s touched_year_months=%s duration_s=%.3f",
            run_id,
            len(touched_year_months),
            manifest_entry["duration_seconds"],
        )
        return json_ready(summary)
    except KeyboardInterrupt:
        finished_at = datetime.now(timezone.utc)
        manifest_entry.update(
            {
                "status": "cancelled",
                "finished_at": finished_at.isoformat(),
                "duration_seconds": duration_seconds(started_at, finished_at),
                "error_summary": "Interrupted by user",
            }
        )
        append_manifest(MANIFEST_PATH, manifest_entry)
        logger.warning("PortWatch GCS publish cancelled run_id=%s", run_id)
        raise
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
        logger.exception("PortWatch GCS publish failed run_id=%s", run_id)
        raise


def _build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Publish the PortWatch bronze/silver slice from local storage to GCS.")
    parser.add_argument("--skip-bronze", action="store_true", help="Do not publish the bronze PortWatch partitions.")
    parser.add_argument("--skip-silver", action="store_true", help="Do not publish the canonical silver PortWatch monthly partitions.")
    parser.add_argument(
        "--include-auxiliary",
        action="store_true",
        help="Also publish the PortWatch scaffold and dimension parquet assets.",
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
        help="Restrict partitioned uploads to a specific YYYY-MM month. Repeat for multiple months.",
    )
    parser.add_argument("--since-year-month", default=None, help="Restrict partitioned uploads to months from YYYY-MM onward.")
    parser.add_argument("--until-year-month", default=None, help="Restrict partitioned uploads to months through YYYY-MM.")
    parser.add_argument("--log-level", default="INFO", help="Logging level, for example INFO or DEBUG.")
    parser.add_argument("--dry-run", action="store_true", help="Show planned uploads without calling GCS.")
    return parser


def main() -> None:
    parser = _build_arg_parser()
    args = parser.parse_args()

    if args.skip_bronze and args.skip_silver:
        parser.error("Choose at least one of bronze or silver to publish.")

    selected_year_months = {_parse_year_month(value) for value in (args.year_month or [])}
    since_year_month = _parse_year_month(args.since_year_month) if args.since_year_month else None
    until_year_month = _parse_year_month(args.until_year_month) if args.until_year_month else None

    summary = publish_portwatch_assets(
        include_bronze=not args.skip_bronze,
        include_silver=not args.skip_silver,
        include_auxiliary=args.include_auxiliary,
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
