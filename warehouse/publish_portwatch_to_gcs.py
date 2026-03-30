from __future__ import annotations

import argparse
import json
import sys
from dataclasses import dataclass
from datetime import date, datetime, timezone
from pathlib import Path, PurePosixPath

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from ingest.common.cloud_config import GcpCloudConfig
from ingest.common.gcs_io import GcsUploadResult, upload_file
from ingest.common.run_artifacts import append_manifest, build_run_id, configure_logger, json_ready


LOGGER_NAME = "portwatch.publish_gcs"
LOG_DIR = PROJECT_ROOT / "logs" / "portwatch"
LOG_PATH = LOG_DIR / "publish_portwatch_to_gcs.log"
MANIFEST_PATH = LOG_DIR / "publish_portwatch_to_gcs_manifest.jsonl"


@dataclass(frozen=True)
class UploadSpec:
    name: str
    local_path: Path
    destination_parts: tuple[str, ...]
    optional: bool = False


def _parse_year_month(value: str) -> str:
    parsed = date.fromisoformat(f"{value}-01")
    return parsed.strftime("%Y-%m")


def _build_specs(include_bronze: bool, include_silver: bool, include_auxiliary: bool) -> list[UploadSpec]:
    specs: list[UploadSpec] = [
        UploadSpec(
            name="metadata_portwatch",
            local_path=PROJECT_ROOT / "data" / "metadata" / "portwatch",
            destination_parts=("metadata", "portwatch"),
            optional=True,
        ),
    ]

    if include_bronze:
        specs.append(
            UploadSpec(
                name="bronze_portwatch",
                local_path=PROJECT_ROOT / "data" / "bronze" / "portwatch",
                destination_parts=("bronze", "portwatch"),
            )
        )

    if include_silver:
        specs.append(
            UploadSpec(
                name="silver_portwatch_monthly",
                local_path=PROJECT_ROOT / "data" / "silver" / "portwatch" / "portwatch_monthly",
                destination_parts=("silver", "portwatch", "portwatch_monthly"),
            )
        )

    if include_auxiliary:
        specs.extend(
            [
                UploadSpec(
                    name="silver_portwatch_dimensions",
                    local_path=PROJECT_ROOT / "data" / "silver" / "portwatch" / "dimensions",
                    destination_parts=("silver", "portwatch", "dimensions"),
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


def _path_year_month(path: Path, root: Path) -> str | None:
    year = None
    month = None
    for part in path.relative_to(root).parts:
        if part.startswith("year="):
            year = int(part.split("=", 1)[1])
        elif part.startswith("month="):
            month = int(part.split("=", 1)[1])
    if year is None or month is None:
        return None
    return f"{year:04d}-{month:02d}"


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


def _candidate_files(
    *,
    local_root: Path,
    include_suffixes: tuple[str, ...],
    selected_year_months: set[str],
    since_year_month: str | None,
    until_year_month: str | None,
) -> list[Path]:
    files = sorted(path for path in local_root.rglob("*") if path.is_file())
    files = [path for path in files if path.suffix in include_suffixes]
    return [
        path
        for path in files
        if _matches_year_month_filters(
            year_month=_path_year_month(path, local_root),
            selected_year_months=selected_year_months,
            since_year_month=since_year_month,
            until_year_month=until_year_month,
        )
    ]


def _status_counts(results: list[GcsUploadResult]) -> dict[str, int]:
    counts = {"uploaded": 0, "skipped_existing": 0, "planned": 0}
    for result in results:
        if result.action in counts:
            counts[result.action] += 1
    return counts


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
            destination_prefix = config.blob_path(*spec.destination_parts)
            if not spec.local_path.exists():
                if spec.optional:
                    uploads[spec.name] = {
                        "status": "skipped_missing_optional_source",
                        "local_path": str(spec.local_path),
                        "gcs_destination": config.gcs_uri(*spec.destination_parts),
                        "files_uploaded": 0,
                        "files_considered": 0,
                        "touched_year_months": [],
                    }
                    continue
                if spec.name == "bronze_portwatch":
                    raise FileNotFoundError(
                        f"Missing required PortWatch source path: {spec.local_path}. "
                        "If you only need the silver slice, rerun with --skip-bronze."
                    )
                raise FileNotFoundError(f"Missing required PortWatch source path: {spec.local_path}")

            if spec.local_path.is_dir():
                candidate_files = _candidate_files(
                    local_root=spec.local_path,
                    include_suffixes=(".parquet",),
                    selected_year_months=selected_year_months,
                    since_year_month=since_year_month,
                    until_year_month=until_year_month,
                )
                results: list[GcsUploadResult] = []
                spec_touched_months = sorted(
                    {
                        year_month
                        for path in candidate_files
                        if (year_month := _path_year_month(path, spec.local_path)) is not None
                    }
                )
                for path in candidate_files:
                    relative_path = path.relative_to(spec.local_path).as_posix()
                    destination_blob_name = str(PurePosixPath(destination_prefix) / PurePosixPath(relative_path))
                    results.append(
                        upload_file(
                            path,
                            bucket_name=config.gcs_bucket,
                            destination_blob_name=destination_blob_name,
                            project_id=config.gcp_project_id,
                            skip_if_exists=skip_existing,
                            dry_run=dry_run,
                        )
                    )

                status_counts = _status_counts(results)
                touched_year_months.update(spec_touched_months)
                uploads[spec.name] = {
                    "status": "completed" if not dry_run else "planned",
                    "local_path": str(spec.local_path),
                    "gcs_destination": config.gcs_uri(*spec.destination_parts),
                    "files_uploaded": status_counts["uploaded"],
                    "files_skipped_existing": status_counts["skipped_existing"],
                    "files_planned": status_counts["planned"],
                    "files_considered": len(results),
                    "touched_year_months": spec_touched_months,
                    "sample_uris": [result.uri for result in results[:10]],
                }
                logger.info(
                    "Spec=%s considered=%s uploaded=%s skipped_existing=%s touched_months=%s",
                    spec.name,
                    len(results),
                    status_counts["uploaded"],
                    status_counts["skipped_existing"],
                    spec_touched_months,
                )
                continue

            result = upload_file(
                spec.local_path,
                bucket_name=config.gcs_bucket,
                destination_blob_name=destination_prefix,
                project_id=config.gcp_project_id,
                skip_if_exists=skip_existing,
                dry_run=dry_run,
            )
            uploads[spec.name] = {
                "status": result.action,
                "local_path": str(spec.local_path),
                "gcs_destination": result.uri,
                "files_uploaded": 1 if result.action == "uploaded" else 0,
                "files_skipped_existing": 1 if result.action == "skipped_existing" else 0,
                "files_planned": 1 if result.action == "planned" else 0,
                "files_considered": 1,
                "touched_year_months": [],
                "sample_uris": [result.uri],
            }
            logger.info("Spec=%s action=%s uri=%s", spec.name, result.action, result.uri)

        finished_at = datetime.now(timezone.utc)
        summary["uploads"] = uploads
        summary["touched_year_months"] = sorted(touched_year_months)
        summary["status"] = "completed" if not dry_run else "planned"
        manifest_entry.update(
            {
                "status": summary["status"],
                "finished_at": finished_at.isoformat(),
                "touched_year_months": sorted(touched_year_months),
                "uploads": uploads,
            }
        )
        append_manifest(MANIFEST_PATH, manifest_entry)
        logger.info("Finished PortWatch GCS publish run_id=%s", run_id)
        return json_ready(summary)
    except Exception as exc:
        finished_at = datetime.now(timezone.utc)
        manifest_entry.update(
            {
                "status": "failed",
                "finished_at": finished_at.isoformat(),
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
