from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
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
    path_year_or_dt_year,
    publish_directory_spec,
    publish_file_spec,
)


LOGGER_NAME = "worldbank_energy.publish_gcs"
LOG_DIR = PROJECT_ROOT / "logs" / "worldbank_energy"
LOG_PATH = LOG_DIR / "publish_worldbank_energy_to_gcs.log"
MANIFEST_PATH = LOG_DIR / "publish_worldbank_energy_to_gcs_manifest.jsonl"


def _parse_year(value: str) -> str:
    year = int(value)
    if year < 1900 or year > 9999:
        raise argparse.ArgumentTypeError(f"Expected a four-digit year, got {value}")
    return f"{year:04d}"


def _build_specs(include_bronze: bool, include_silver: bool, include_metadata: bool) -> list[UploadSpec]:
    specs: list[UploadSpec] = []

    if include_metadata:
        specs.append(
            UploadSpec(
                name="metadata_worldbank_energy",
                local_path=PROJECT_ROOT / "data" / "metadata" / "worldbank_energy",
                destination_parts=("metadata", "worldbank_energy"),
                include_suffixes=(".json",),
                optional=True,
            )
        )

    if include_bronze:
        specs.append(
            UploadSpec(
                name="bronze_worldbank_energy",
                local_path=PROJECT_ROOT / "data" / "bronze" / "worldbank_energy",
                destination_parts=("bronze", "worldbank_energy"),
                include_suffixes=(".jsonl", ".csv"),
                partition_value_resolver=path_year_or_dt_year,
            )
        )

    if include_silver:
        specs.append(
            UploadSpec(
                name="silver_worldbank_energy",
                local_path=PROJECT_ROOT / "data" / "silver" / "worldbank_energy" / "energy_vulnerability",
                destination_parts=("silver", "worldbank_energy", "energy_vulnerability"),
                include_suffixes=(".parquet",),
                partition_value_resolver=path_year_or_dt_year,
            )
        )

    return specs


def publish_worldbank_energy_assets(
    *,
    include_bronze: bool,
    include_silver: bool,
    include_metadata: bool,
    skip_existing: bool,
    selected_years: set[str],
    since_year: str | None,
    until_year: str | None,
    dry_run: bool,
    log_level: str,
) -> dict[str, object]:
    config = GcpCloudConfig.from_env()
    specs = _build_specs(
        include_bronze=include_bronze,
        include_silver=include_silver,
        include_metadata=include_metadata,
    )
    run_id = build_run_id("worldbank_energy_publish_gcs")
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
        "selected_years": sorted(selected_years),
        "since_year": since_year,
        "until_year": until_year,
        "dry_run": dry_run,
        "uploads": {},
        "log_path": str(LOG_PATH),
        "manifest_path": str(MANIFEST_PATH),
    }
    manifest_entry: dict[str, object] = {
        "run_id": run_id,
        "asset_name": "publish_worldbank_energy_to_gcs",
        "dataset_name": "worldbank_energy",
        "status": "running",
        "started_at": started_at.isoformat(),
        "finished_at": None,
        "duration_seconds": None,
        "skip_existing": skip_existing,
        "selected_years": sorted(selected_years),
        "since_year": since_year,
        "until_year": until_year,
        "dry_run": dry_run,
        "touched_years": [],
        "uploads": {},
        "error_summary": None,
    }

    try:
        logger.info("Starting World Bank energy GCS publish run_id=%s", run_id)
        logger.info("Publishing into gs://%s/%s", config.gcs_bucket, config.gcs_prefix)
        touched_years: set[str] = set()
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
                        "touched_years": [],
                        "sample_uris": [],
                        "sample_results": [],
                    }
                    continue
                if spec.name == "bronze_worldbank_energy":
                    raise FileNotFoundError(
                        f"Missing required World Bank energy source path: {spec.local_path}. "
                        "If you only need the silver slice, rerun with --skip-bronze."
                    )
                raise FileNotFoundError(f"Missing required World Bank energy source path: {spec.local_path}")

            if spec.local_path.is_dir():
                upload_summary, _, spec_touched_years = publish_directory_spec(
                    spec=spec,
                    config=config,
                    skip_existing=skip_existing,
                    dry_run=dry_run,
                    selected_partition_values=selected_years,
                    since_partition_value=since_year,
                    until_partition_value=until_year,
                    logger=logger,
                )
                touched_years.update(spec_touched_years)
                uploads[spec.name] = {
                    **upload_summary,
                    "touched_years": spec_touched_years,
                }
                logger.info(
                    "Spec=%s considered=%s uploaded=%s skipped_existing=%s checksum_matched=%s checksum_mismatched=%s touched_years=%s",
                    spec.name,
                    upload_summary["files_considered"],
                    upload_summary["files_uploaded"],
                    upload_summary["files_skipped_existing"],
                    upload_summary["files_checksum_matched"],
                    upload_summary["files_checksum_mismatched"],
                    spec_touched_years,
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
                "touched_years": [],
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
        summary["touched_years"] = sorted(touched_years)
        summary["status"] = "completed" if not dry_run else "planned"
        summary["duration_seconds"] = duration_seconds(started_at, finished_at)
        manifest_entry.update(
            {
                "status": summary["status"],
                "finished_at": finished_at.isoformat(),
                "duration_seconds": duration_seconds(started_at, finished_at),
                "touched_years": sorted(touched_years),
                "uploads": uploads,
            }
        )
        append_manifest(MANIFEST_PATH, manifest_entry)
        logger.info(
            "Finished World Bank energy GCS publish run_id=%s touched_years=%s duration_s=%.3f",
            run_id,
            len(touched_years),
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
        logger.warning("World Bank energy GCS publish cancelled run_id=%s", run_id)
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
        logger.exception("World Bank energy GCS publish failed run_id=%s", run_id)
        raise


def _build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Publish the World Bank energy bronze/silver slice from local storage to GCS."
    )
    parser.add_argument("--skip-bronze", action="store_true", help="Do not publish the bronze World Bank energy folders.")
    parser.add_argument("--skip-silver", action="store_true", help="Do not publish the silver World Bank energy parquet folders.")
    parser.add_argument("--skip-metadata", action="store_true", help="Do not publish the World Bank energy metadata JSON files.")
    parser.add_argument(
        "--overwrite-existing",
        action="store_true",
        help="Re-upload files even when the destination blob already exists.",
    )
    parser.add_argument(
        "--year",
        action="append",
        default=None,
        help="Restrict partitioned uploads to a specific year. Repeat for multiple years.",
    )
    parser.add_argument("--since-year", default=None, help="Restrict partitioned uploads to years from YYYY onward.")
    parser.add_argument("--until-year", default=None, help="Restrict partitioned uploads to years through YYYY.")
    parser.add_argument("--log-level", default="INFO", help="Logging level, for example INFO or DEBUG.")
    parser.add_argument("--dry-run", action="store_true", help="Show planned uploads without calling GCS.")
    return parser


def main() -> None:
    parser = _build_arg_parser()
    args = parser.parse_args()

    if args.skip_bronze and args.skip_silver and args.skip_metadata:
        parser.error("Choose at least one of bronze, silver, or metadata to publish.")

    selected_years = {_parse_year(value) for value in (args.year or [])}
    since_year = _parse_year(args.since_year) if args.since_year else None
    until_year = _parse_year(args.until_year) if args.until_year else None

    summary = publish_worldbank_energy_assets(
        include_bronze=not args.skip_bronze,
        include_silver=not args.skip_silver,
        include_metadata=not args.skip_metadata,
        skip_existing=not args.overwrite_existing,
        selected_years=selected_years,
        since_year=since_year,
        until_year=until_year,
        dry_run=args.dry_run,
        log_level=args.log_level,
    )
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
