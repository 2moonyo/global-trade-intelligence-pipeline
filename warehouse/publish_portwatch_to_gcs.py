from __future__ import annotations

import argparse
import json
from dataclasses import dataclass
from pathlib import Path

from ingest.common.cloud_config import GcpCloudConfig
from ingest.common.gcs_io import upload_file, upload_tree


PROJECT_ROOT = Path(__file__).resolve().parents[1]


@dataclass(frozen=True)
class UploadSpec:
    name: str
    local_path: Path
    destination_parts: tuple[str, ...]
    optional: bool = False


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


def publish_portwatch_assets(
    *,
    include_bronze: bool,
    include_silver: bool,
    include_auxiliary: bool,
    dry_run: bool,
) -> dict[str, object]:
    config = GcpCloudConfig.from_env()
    specs = _build_specs(include_bronze=include_bronze, include_silver=include_silver, include_auxiliary=include_auxiliary)

    summary: dict[str, object] = {
        "project_id": config.gcp_project_id,
        "bucket": config.gcs_bucket,
        "gcs_prefix": config.gcs_prefix,
        "dry_run": dry_run,
        "uploads": {},
    }

    uploads: dict[str, object] = {}
    for spec in specs:
        destination = config.blob_path(*spec.destination_parts)
        if not spec.local_path.exists():
            if spec.optional:
                uploads[spec.name] = {
                    "status": "skipped_missing_optional_source",
                    "local_path": str(spec.local_path),
                    "gcs_destination": config.gcs_uri(*spec.destination_parts),
                    "files_uploaded": 0,
                }
                continue
            raise FileNotFoundError(f"Missing required PortWatch source path: {spec.local_path}")

        if spec.local_path.is_dir():
            uris = upload_tree(
                spec.local_path,
                bucket_name=config.gcs_bucket,
                destination_prefix=destination,
                project_id=config.gcp_project_id,
                include_suffixes=(".parquet",),
                dry_run=dry_run,
            )
            uploads[spec.name] = {
                "status": "uploaded" if not dry_run else "planned",
                "local_path": str(spec.local_path),
                "gcs_destination": config.gcs_uri(*spec.destination_parts),
                "files_uploaded": len(uris),
                "sample_uris": uris[:10],
            }
            continue

        uri = upload_file(
            spec.local_path,
            bucket_name=config.gcs_bucket,
            destination_blob_name=destination,
            project_id=config.gcp_project_id,
            dry_run=dry_run,
        )
        uploads[spec.name] = {
            "status": "uploaded" if not dry_run else "planned",
            "local_path": str(spec.local_path),
            "gcs_destination": uri,
            "files_uploaded": 1,
            "sample_uris": [uri],
        }

    summary["uploads"] = uploads
    return summary


def _build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Publish the PortWatch bronze/silver slice from local storage to GCS.")
    parser.add_argument("--skip-bronze", action="store_true", help="Do not publish the bronze PortWatch partitions.")
    parser.add_argument("--skip-silver", action="store_true", help="Do not publish the canonical silver PortWatch monthly partitions.")
    parser.add_argument(
        "--include-auxiliary",
        action="store_true",
        help="Also publish the PortWatch scaffold and dimension parquet assets.",
    )
    parser.add_argument("--dry-run", action="store_true", help="Show planned uploads without calling GCS.")
    return parser


def main() -> None:
    parser = _build_arg_parser()
    args = parser.parse_args()

    if args.skip_bronze and args.skip_silver:
        parser.error("Choose at least one of bronze or silver to publish.")

    summary = publish_portwatch_assets(
        include_bronze=not args.skip_bronze,
        include_silver=not args.skip_silver,
        include_auxiliary=args.include_auxiliary,
        dry_run=args.dry_run,
    )
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
