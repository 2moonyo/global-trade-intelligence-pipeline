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
    path_year_or_dt_year,
    publish_directory_spec,
    publish_file_spec,
)


LOGGER_NAME = "comtrade.publish_gcs"
LOG_DIR = PROJECT_ROOT / "logs" / "comtrade"
LOG_PATH = LOG_DIR / "publish_comtrade_to_gcs.log"
MANIFEST_PATH = LOG_DIR / "publish_comtrade_to_gcs_manifest.jsonl"


def _parse_year_month(value: str) -> str:
    parsed = date.fromisoformat(f"{value}-01")
    return parsed.strftime("%Y-%m")


def _selected_years_from_months(selected_year_months: set[str]) -> set[str]:
    return {value[:4] for value in selected_year_months}


def _build_specs(
    *,
    include_metadata: bool,
    include_bronze: bool,
    include_fact: bool,
    include_dimensions: bool,
    include_routing: bool,
    include_audit: bool,
) -> dict[str, UploadSpec]:
    specs: dict[str, UploadSpec] = {}

    if include_metadata:
        specs["metadata_comtrade"] = UploadSpec(
            name="metadata_comtrade",
            local_path=PROJECT_ROOT / "data" / "metadata" / "comtrade",
            destination_parts=("metadata", "comtrade"),
            include_suffixes=(".json", ".csv", ".parquet"),
            optional=True,
        )

    if include_bronze:
        specs["bronze_comtrade"] = UploadSpec(
            name="bronze_comtrade",
            local_path=PROJECT_ROOT / "data" / "bronze" / "comtrade" / "monthly_history",
            destination_parts=("bronze", "comtrade", "monthly_history"),
            include_suffixes=(".json",),
            partition_value_resolver=path_year_or_dt_year,
        )

    if include_fact:
        specs["silver_comtrade_fact"] = UploadSpec(
            name="silver_comtrade_fact",
            local_path=PROJECT_ROOT / "data" / "silver" / "comtrade" / "comtrade_fact",
            destination_parts=("silver", "comtrade", "comtrade_fact"),
            include_suffixes=(".parquet",),
            partition_value_resolver=path_year_month,
        )

    if include_dimensions:
        specs["silver_comtrade_dimensions"] = UploadSpec(
            name="silver_comtrade_dimensions",
            local_path=PROJECT_ROOT / "data" / "silver" / "comtrade" / "dimensions",
            destination_parts=("silver", "comtrade", "dimensions"),
            include_suffixes=(".parquet",),
            optional=True,
        )

    if include_routing:
        specs["silver_comtrade_routes"] = UploadSpec(
            name="silver_comtrade_routes",
            local_path=PROJECT_ROOT / "data" / "silver" / "comtrade" / "dim_trade_routes.parquet",
            destination_parts=("silver", "comtrade", "dim_trade_routes.parquet"),
            optional=True,
        )

    if include_audit:
        specs["audit_comtrade"] = UploadSpec(
            name="audit_comtrade",
            local_path=PROJECT_ROOT / "data" / "metadata" / "comtrade" / "ingest_reports",
            destination_parts=("metadata", "comtrade", "ingest_reports"),
            include_suffixes=(".json", ".jsonl", ".csv", ".parquet"),
            optional=True,
        )

    return specs


def publish_comtrade_assets(
    *,
    include_metadata: bool,
    include_bronze: bool,
    include_fact: bool,
    include_dimensions: bool,
    include_routing: bool,
    include_audit: bool,
    skip_existing: bool,
    selected_year_months: set[str],
    since_year_month: str | None,
    until_year_month: str | None,
    dry_run: bool,
    log_level: str,
) -> dict[str, object]:
    config = GcpCloudConfig.from_env()
    specs = _build_specs(
        include_metadata=include_metadata,
        include_bronze=include_bronze,
        include_fact=include_fact,
        include_dimensions=include_dimensions,
        include_routing=include_routing,
        include_audit=include_audit,
    )
    run_id = build_run_id("comtrade_publish_gcs")
    logger = configure_logger(
        logger_name=LOGGER_NAME,
        log_path=LOG_PATH,
        log_level=log_level,
    )
    started_at = datetime.now(timezone.utc)

    selected_years = _selected_years_from_months(selected_year_months)
    since_year = since_year_month[:4] if since_year_month else None
    until_year = until_year_month[:4] if until_year_month else None

    manifest_entry: dict[str, object] = {
        "run_id": run_id,
        "asset_name": "publish_comtrade_to_gcs",
        "dataset_name": "comtrade",
        "status": "running",
        "started_at": started_at.isoformat(),
        "finished_at": None,
        "duration_seconds": None,
        "skip_existing": skip_existing,
        "selected_year_months": sorted(selected_year_months),
        "since_year_month": since_year_month,
        "until_year_month": until_year_month,
        "dry_run": dry_run,
        "uploads": {},
        "touched_year_months": [],
        "error_summary": None,
    }

    try:
        logger.info("Step 1/1 Publish selected Comtrade asset families to GCS")
        uploads: dict[str, object] = {}
        touched_year_months: set[str] = set()

        for name, spec in specs.items():
            if not spec.local_path.exists():
                if spec.optional:
                    uploads[name] = {
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
                raise FileNotFoundError(f"Missing required Comtrade source path: {spec.local_path}")

            if spec.local_path.is_dir():
                if name == "silver_comtrade_fact":
                    upload_summary, _, spec_touched = publish_directory_spec(
                        spec=spec,
                        config=config,
                        skip_existing=skip_existing,
                        dry_run=dry_run,
                        selected_partition_values=selected_year_months,
                        since_partition_value=since_year_month,
                        until_partition_value=until_year_month,
                        logger=logger,
                    )
                    touched_year_months.update(spec_touched)
                    uploads[name] = {**upload_summary, "touched_year_months": spec_touched}
                    logger.info(
                        "Spec=%s considered=%s uploaded=%s skipped_existing=%s checksum_matched=%s checksum_mismatched=%s touched_year_months=%s",
                        name,
                        upload_summary["files_considered"],
                        upload_summary["files_uploaded"],
                        upload_summary["files_skipped_existing"],
                        upload_summary["files_checksum_matched"],
                        upload_summary["files_checksum_mismatched"],
                        spec_touched,
                    )
                elif name == "bronze_comtrade":
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
                    uploads[name] = {
                        **upload_summary,
                        "touched_years": spec_touched_years,
                    }
                    logger.info(
                        "Spec=%s considered=%s uploaded=%s skipped_existing=%s checksum_matched=%s checksum_mismatched=%s touched_years=%s",
                        name,
                        upload_summary["files_considered"],
                        upload_summary["files_uploaded"],
                        upload_summary["files_skipped_existing"],
                        upload_summary["files_checksum_matched"],
                        upload_summary["files_checksum_mismatched"],
                        spec_touched_years,
                    )
                else:
                    upload_summary, _, _ = publish_directory_spec(
                        spec=spec,
                        config=config,
                        skip_existing=skip_existing,
                        dry_run=dry_run,
                        selected_partition_values=set(),
                        since_partition_value=None,
                        until_partition_value=None,
                        logger=logger,
                    )
                    uploads[name] = {**upload_summary, "touched_year_months": []}
                    logger.info(
                        "Spec=%s considered=%s uploaded=%s skipped_existing=%s checksum_matched=%s checksum_mismatched=%s",
                        name,
                        upload_summary["files_considered"],
                        upload_summary["files_uploaded"],
                        upload_summary["files_skipped_existing"],
                        upload_summary["files_checksum_matched"],
                        upload_summary["files_checksum_mismatched"],
                    )
                continue

            upload_summary = publish_file_spec(
                spec=spec,
                config=config,
                skip_existing=skip_existing,
                dry_run=dry_run,
            )
            uploads[name] = {**upload_summary, "touched_year_months": []}
            logger.info(
                "Spec=%s action=%s checksum_match=%s upload_reason=%s uri=%s",
                name,
                upload_summary["status"],
                upload_summary["checksum_match"],
                upload_summary["upload_reason"],
                upload_summary["gcs_destination"],
            )

        manifest_entry["status"] = "planned" if dry_run else "completed"
        finished_at = datetime.now(timezone.utc)
        manifest_entry["finished_at"] = finished_at.isoformat()
        manifest_entry["duration_seconds"] = duration_seconds(started_at, finished_at)
        manifest_entry["uploads"] = uploads
        manifest_entry["touched_year_months"] = sorted(touched_year_months)
        append_manifest(MANIFEST_PATH, manifest_entry)
        logger.info(
            "Finished Comtrade GCS publish run_id=%s touched_year_months=%s duration_s=%.3f",
            run_id,
            len(touched_year_months),
            manifest_entry["duration_seconds"],
        )
        return json_ready(manifest_entry)
    except KeyboardInterrupt:
        manifest_entry["status"] = "cancelled"
        finished_at = datetime.now(timezone.utc)
        manifest_entry["finished_at"] = finished_at.isoformat()
        manifest_entry["duration_seconds"] = duration_seconds(started_at, finished_at)
        manifest_entry["error_summary"] = "Interrupted by user"
        append_manifest(MANIFEST_PATH, manifest_entry)
        logger.warning("Comtrade GCS publish cancelled run_id=%s", run_id)
        raise
    except Exception as exc:
        manifest_entry["status"] = "failed"
        finished_at = datetime.now(timezone.utc)
        manifest_entry["finished_at"] = finished_at.isoformat()
        manifest_entry["duration_seconds"] = duration_seconds(started_at, finished_at)
        manifest_entry["error_summary"] = str(exc)
        append_manifest(MANIFEST_PATH, manifest_entry)
        logger.exception("Comtrade GCS publish failed run_id=%s", run_id)
        raise


def _build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Publish Comtrade bronze, silver, and audit assets to GCS.")
    parser.add_argument("--skip-metadata", action="store_true")
    parser.add_argument("--skip-bronze", action="store_true")
    parser.add_argument("--skip-fact", action="store_true")
    parser.add_argument("--skip-dimensions", action="store_true")
    parser.add_argument("--skip-routing", action="store_true")
    parser.add_argument("--skip-audit", action="store_true")
    parser.add_argument("--overwrite-existing", action="store_true")
    parser.add_argument("--year-month", action="append", default=None, help="Restrict fact uploads to YYYY-MM month slices.")
    parser.add_argument("--since-year-month", default=None)
    parser.add_argument("--until-year-month", default=None)
    parser.add_argument("--log-level", default="INFO")
    parser.add_argument("--dry-run", action="store_true")
    return parser


def main() -> None:
    parser = _build_arg_parser()
    args = parser.parse_args()
    if all(
        [
            args.skip_metadata,
            args.skip_bronze,
            args.skip_fact,
            args.skip_dimensions,
            args.skip_routing,
            args.skip_audit,
        ]
    ):
        parser.error("Choose at least one Comtrade asset family to publish.")

    selected_year_months = {_parse_year_month(value) for value in (args.year_month or [])}
    since_year_month = _parse_year_month(args.since_year_month) if args.since_year_month else None
    until_year_month = _parse_year_month(args.until_year_month) if args.until_year_month else None

    summary = publish_comtrade_assets(
        include_metadata=not args.skip_metadata,
        include_bronze=not args.skip_bronze,
        include_fact=not args.skip_fact,
        include_dimensions=not args.skip_dimensions,
        include_routing=not args.skip_routing,
        include_audit=not args.skip_audit,
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
