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
from warehouse.gcs_publish_common import UploadSpec, path_year_month, publish_directory_spec


LOGGER_NAME = "fx.publish_gcs"
LOG_DIR = PROJECT_ROOT / "logs" / "fx"
LOG_PATH = LOG_DIR / "publish_fx_to_gcs.log"
MANIFEST_PATH = LOG_DIR / "publish_fx_to_gcs_manifest.jsonl"


def _parse_year_month(value: str) -> str:
    parsed = date.fromisoformat(f"{value}-01")
    return parsed.strftime("%Y-%m")


def _path_year_month_or_dt(path: Path, root: Path) -> str | None:
    year_month = path_year_month(path, root)
    if year_month is not None:
        return year_month
    for part in path.relative_to(root).parts:
        if part.startswith("dt="):
            return part.split("=", 1)[1][:7]
    return None


def _build_specs(*, include_bronze: bool, include_monthly: bool) -> list[UploadSpec]:
    specs: list[UploadSpec] = []
    if include_bronze:
        specs.append(
            UploadSpec(
                name="bronze_fx",
                local_path=PROJECT_ROOT / "data" / "bronze" / "ecb_fx_eu",
                destination_parts=("bronze", "ecb_fx_eu"),
                include_suffixes=(".csv",),
                partition_value_resolver=_path_year_month_or_dt,
            )
        )
    if include_monthly:
        specs.append(
            UploadSpec(
                name="silver_fx_monthly",
                local_path=PROJECT_ROOT / "data" / "silver" / "fx" / "ecb_fx_eu_monthly",
                destination_parts=("silver", "fx", "ecb_fx_eu_monthly"),
                include_suffixes=(".parquet",),
                partition_value_resolver=path_year_month,
            )
        )
    return specs


def publish_fx_assets(
    *,
    include_bronze: bool,
    include_monthly: bool,
    skip_existing: bool,
    selected_year_months: set[str],
    since_year_month: str | None,
    until_year_month: str | None,
    dry_run: bool,
    log_level: str,
) -> dict[str, object]:
    config = GcpCloudConfig.from_env()
    specs = _build_specs(include_bronze=include_bronze, include_monthly=include_monthly)
    run_id = build_run_id("fx_publish_gcs")
    logger = configure_logger(logger_name=LOGGER_NAME, log_path=LOG_PATH, log_level=log_level)
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
        "asset_name": "publish_fx_to_gcs",
        "dataset_name": "fx",
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
        logger.info("Starting FX GCS publish run_id=%s", run_id)
        uploads: dict[str, object] = {}
        touched_year_months: set[str] = set()

        for spec in specs:
            if not spec.local_path.exists():
                raise FileNotFoundError(f"Missing required FX source path: {spec.local_path}")
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

        finished_at = datetime.now(timezone.utc)
        summary.update(
            {
                "uploads": uploads,
                "touched_year_months": sorted(touched_year_months),
                "status": "completed" if not dry_run else "planned",
                "duration_seconds": duration_seconds(started_at, finished_at),
            }
        )
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
        logger.exception("FX GCS publish failed run_id=%s", run_id)
        raise


def main() -> None:
    parser = argparse.ArgumentParser(description="Publish FX bronze and monthly silver assets to GCS with checksum-aware skipping.")
    parser.add_argument("--skip-bronze", action="store_true", help="Skip publishing FX bronze CSV files.")
    parser.add_argument("--skip-monthly", action="store_true", help="Skip publishing FX monthly silver parquet partitions.")
    parser.add_argument("--year-month", action="append", default=None, help="Restrict publish to a specific YYYY-MM month.")
    parser.add_argument("--since-year-month", default=None, help="Restrict publish to months from YYYY-MM onward.")
    parser.add_argument("--until-year-month", default=None, help="Restrict publish to months through YYYY-MM.")
    parser.add_argument("--log-level", default="INFO")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    selected_year_months = {_parse_year_month(value) for value in (args.year_month or [])}
    since_year_month = _parse_year_month(args.since_year_month) if args.since_year_month else None
    until_year_month = _parse_year_month(args.until_year_month) if args.until_year_month else None
    summary = publish_fx_assets(
        include_bronze=not args.skip_bronze,
        include_monthly=not args.skip_monthly,
        skip_existing=True,
        selected_year_months=selected_year_months,
        since_year_month=since_year_month,
        until_year_month=until_year_month,
        dry_run=args.dry_run,
        log_level=args.log_level,
    )
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
