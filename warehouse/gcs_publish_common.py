from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from typing import Callable

from ingest.common.cloud_config import GcpCloudConfig
from ingest.common.gcs_io import GcsUploadResult, upload_file


PartitionValueResolver = Callable[[Path, Path], str | None]


@dataclass(frozen=True)
class UploadSpec:
    name: str
    local_path: Path
    destination_parts: tuple[str, ...]
    include_suffixes: tuple[str, ...] = ()
    optional: bool = False
    partition_value_resolver: PartitionValueResolver | None = None


def path_year_month(path: Path, root: Path) -> str | None:
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


def path_year_or_dt_year(path: Path, root: Path) -> str | None:
    for part in path.relative_to(root).parts:
        if part.startswith("year="):
            return f"{int(part.split('=', 1)[1]):04d}"
        if part.startswith("dt="):
            return part.split("=", 1)[1][:4]
    return None


def matches_partition_filters(
    *,
    partition_value: str | None,
    selected_partition_values: set[str],
    since_partition_value: str | None,
    until_partition_value: str | None,
) -> bool:
    if partition_value is None:
        return True
    if selected_partition_values and partition_value not in selected_partition_values:
        return False
    if since_partition_value and partition_value < since_partition_value:
        return False
    if until_partition_value and partition_value > until_partition_value:
        return False
    return True


def candidate_files(
    *,
    local_root: Path,
    include_suffixes: tuple[str, ...],
    selected_partition_values: set[str],
    since_partition_value: str | None,
    until_partition_value: str | None,
    partition_value_resolver: PartitionValueResolver | None,
) -> list[Path]:
    files = sorted(path for path in local_root.rglob("*") if path.is_file())
    if include_suffixes:
        files = [path for path in files if path.suffix in include_suffixes]
    return [
        path
        for path in files
        if matches_partition_filters(
            partition_value=partition_value_resolver(path, local_root) if partition_value_resolver else None,
            selected_partition_values=selected_partition_values,
            since_partition_value=since_partition_value,
            until_partition_value=until_partition_value,
        )
    ]


def status_counts(results: list[GcsUploadResult]) -> dict[str, int]:
    counts = {"uploaded": 0, "skipped_existing": 0, "planned": 0}
    for result in results:
        if result.action in counts:
            counts[result.action] += 1
    return counts


def publish_directory_spec(
    *,
    spec: UploadSpec,
    config: GcpCloudConfig,
    skip_existing: bool,
    dry_run: bool,
    selected_partition_values: set[str],
    since_partition_value: str | None,
    until_partition_value: str | None,
) -> tuple[dict[str, object], list[GcsUploadResult], list[str]]:
    destination_prefix = config.blob_path(*spec.destination_parts)
    files = candidate_files(
        local_root=spec.local_path,
        include_suffixes=spec.include_suffixes,
        selected_partition_values=selected_partition_values,
        since_partition_value=since_partition_value,
        until_partition_value=until_partition_value,
        partition_value_resolver=spec.partition_value_resolver,
    )
    touched_partition_values = sorted(
        {
            value
            for path in files
            if spec.partition_value_resolver
            if (value := spec.partition_value_resolver(path, spec.local_path)) is not None
        }
    )

    results: list[GcsUploadResult] = []
    for path in files:
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

    counts = status_counts(results)
    summary = {
        "status": "completed" if not dry_run else "planned",
        "local_path": str(spec.local_path),
        "gcs_destination": config.gcs_uri(*spec.destination_parts),
        "files_uploaded": counts["uploaded"],
        "files_skipped_existing": counts["skipped_existing"],
        "files_planned": counts["planned"],
        "files_considered": len(results),
        "touched_partition_values": touched_partition_values,
        "sample_uris": [result.uri for result in results[:10]],
    }
    return summary, results, touched_partition_values


def publish_file_spec(
    *,
    spec: UploadSpec,
    config: GcpCloudConfig,
    skip_existing: bool,
    dry_run: bool,
) -> dict[str, object]:
    destination_blob_name = config.blob_path(*spec.destination_parts)
    result = upload_file(
        spec.local_path,
        bucket_name=config.gcs_bucket,
        destination_blob_name=destination_blob_name,
        project_id=config.gcp_project_id,
        skip_if_exists=skip_existing,
        dry_run=dry_run,
    )
    return {
        "status": result.action,
        "local_path": str(spec.local_path),
        "gcs_destination": result.uri,
        "files_uploaded": 1 if result.action == "uploaded" else 0,
        "files_skipped_existing": 1 if result.action == "skipped_existing" else 0,
        "files_planned": 1 if result.action == "planned" else 0,
        "files_considered": 1,
        "touched_partition_values": [],
        "sample_uris": [result.uri],
    }
