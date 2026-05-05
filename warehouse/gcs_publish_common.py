from __future__ import annotations

from dataclasses import dataclass, replace
from pathlib import Path, PurePosixPath
from typing import Callable

from ingest.common.cloud_config import GcpCloudConfig
from ingest.common.gcs_io import (
    GcsBlobMetadata,
    GcsUploadResult,
    file_md5_base64,
    list_blob_metadata,
    upload_file,
)
from ingest.common.run_artifacts import iter_progress


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
    counts = {
        "uploaded": 0,
        "skipped_existing": 0,
        "planned": 0,
        "checksum_verified": 0,
        "checksum_matched": 0,
        "checksum_mismatched": 0,
        "checksum_unverified": 0,
        "uploaded_missing_remote": 0,
        "uploaded_checksum_mismatch": 0,
        "uploaded_remote_checksum_unavailable": 0,
    }
    for result in results:
        if result.action in counts:
            counts[result.action] += 1
        if result.checksum_verified:
            counts["checksum_verified"] += 1
            if result.checksum_match is True:
                counts["checksum_matched"] += 1
            elif result.checksum_match is False:
                counts["checksum_mismatched"] += 1
        else:
            counts["checksum_unverified"] += 1

        if result.action == "uploaded":
            if result.upload_reason == "missing_remote":
                counts["uploaded_missing_remote"] += 1
            elif result.upload_reason == "checksum_mismatch":
                counts["uploaded_checksum_mismatch"] += 1
            elif result.upload_reason == "remote_checksum_unavailable":
                counts["uploaded_remote_checksum_unavailable"] += 1
    return counts


def _sample_result(result: GcsUploadResult) -> dict[str, object]:
    return {
        "uri": result.uri,
        "action": result.action,
        "checksum_verified": result.checksum_verified,
        "checksum_match": result.checksum_match,
        "upload_reason": result.upload_reason,
    }


def _upload_with_checksum_awareness(
    *,
    local_path: Path,
    config: GcpCloudConfig,
    destination_blob_name: str,
    dry_run: bool,
    existing_blob: GcsBlobMetadata | None,
    local_md5_cache: dict[Path, str],
) -> GcsUploadResult:
    uri = f"gs://{config.gcs_bucket}/{destination_blob_name}"
    if dry_run:
        return GcsUploadResult(uri=uri, action="planned", upload_reason="dry_run")

    if existing_blob is None:
        uploaded = upload_file(
            local_path,
            bucket_name=config.gcs_bucket,
            destination_blob_name=destination_blob_name,
            project_id=config.gcp_project_id,
            skip_if_exists=False,
            dry_run=False,
        )
        return replace(uploaded, upload_reason="missing_remote")

    if existing_blob.md5_hash:
        local_md5 = local_md5_cache.get(local_path)
        if local_md5 is None:
            local_md5 = file_md5_base64(local_path)
            local_md5_cache[local_path] = local_md5
        if local_md5 == existing_blob.md5_hash:
            return GcsUploadResult(
                uri=uri,
                action="skipped_existing",
                checksum_verified=True,
                checksum_match=True,
                upload_reason="checksum_match",
                local_md5_base64=local_md5,
                remote_md5_base64=existing_blob.md5_hash,
            )

        uploaded = upload_file(
            local_path,
            bucket_name=config.gcs_bucket,
            destination_blob_name=destination_blob_name,
            project_id=config.gcp_project_id,
            skip_if_exists=False,
            dry_run=False,
        )
        return replace(
            uploaded,
            checksum_verified=True,
            checksum_match=False,
            upload_reason="checksum_mismatch",
            local_md5_base64=local_md5,
            remote_md5_base64=existing_blob.md5_hash,
        )

    uploaded = upload_file(
        local_path,
        bucket_name=config.gcs_bucket,
        destination_blob_name=destination_blob_name,
        project_id=config.gcp_project_id,
        skip_if_exists=False,
        dry_run=False,
    )
    return replace(uploaded, upload_reason="remote_checksum_unavailable")


def publish_directory_spec(
    *,
    spec: UploadSpec,
    config: GcpCloudConfig,
    skip_existing: bool,
    dry_run: bool,
    selected_partition_values: set[str],
    since_partition_value: str | None,
    until_partition_value: str | None,
    logger=None,
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
    if logger is not None:
        logger.info(
            "Publishing spec=%s files=%s destination=%s touched_partitions=%s",
            spec.name,
            len(files),
            config.gcs_uri(*spec.destination_parts),
            touched_partition_values[:20],
        )

    existing_blob_metadata: dict[str, GcsBlobMetadata] = {}
    if skip_existing and not dry_run:
        existing_blob_metadata = list_blob_metadata(
            bucket_name=config.gcs_bucket,
            prefix=destination_prefix,
            project_id=config.gcp_project_id,
        )
        if logger is not None:
            logger.info(
                "Prefetched existing GCS objects for spec=%s count=%s prefix=%s",
                spec.name,
                len(existing_blob_metadata),
                config.gcs_uri(*spec.destination_parts),
            )

    results: list[GcsUploadResult] = []
    local_md5_cache: dict[Path, str] = {}
    partition_groups: list[tuple[str | None, list[Path]]] = []
    if spec.partition_value_resolver:
        files_by_partition: dict[str | None, list[Path]] = {}
        for path in files:
            partition_value = spec.partition_value_resolver(path, spec.local_path)
            files_by_partition.setdefault(partition_value, []).append(path)
        partition_groups = sorted(
            files_by_partition.items(),
            key=lambda item: (item[0] is None, item[0] or ""),
        )
    else:
        partition_groups = [(None, files)]

    partition_iterable = iter_progress(
        partition_groups,
        desc=f"GCS {spec.name} partitions",
        total=len(partition_groups),
        unit="partition",
    )
    for partition_value, grouped_files in partition_iterable:
        partition_results: list[GcsUploadResult] = []
        if logger is not None and spec.partition_value_resolver:
            logger.info(
                "Uploading spec=%s partition=%s files=%s",
                spec.name,
                partition_value,
                len(grouped_files),
            )
        file_iterable = iter_progress(
            grouped_files,
            desc=f"GCS {spec.name}" + (f" {partition_value}" if partition_value else ""),
            total=len(grouped_files),
            unit="file",
        )
        for path in file_iterable:
            relative_path = path.relative_to(spec.local_path).as_posix()
            destination_blob_name = str(PurePosixPath(destination_prefix) / PurePosixPath(relative_path))
            result = _upload_with_checksum_awareness(
                local_path=path,
                config=config,
                destination_blob_name=destination_blob_name,
                dry_run=dry_run,
                existing_blob=existing_blob_metadata.get(destination_blob_name),
                local_md5_cache=local_md5_cache,
            )
            if result.action == "uploaded":
                existing_blob_metadata[destination_blob_name] = GcsBlobMetadata(
                    name=destination_blob_name,
                    uri=result.uri,
                    md5_hash=result.local_md5_base64,
                )
            partition_results.append(result)
            results.append(result)
        if logger is not None and spec.partition_value_resolver:
            partition_counts = status_counts(partition_results)
            logger.info(
                "Finished spec=%s partition=%s uploaded=%s skipped_existing=%s checksum_matched=%s checksum_mismatched=%s planned=%s",
                spec.name,
                partition_value,
                partition_counts["uploaded"],
                partition_counts["skipped_existing"],
                partition_counts["checksum_matched"],
                partition_counts["checksum_mismatched"],
                partition_counts["planned"],
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
        "checksum_aware": True,
        "checksum_verified_file_count": counts["checksum_verified"],
        "files_checksum_matched": counts["checksum_matched"],
        "files_checksum_mismatched": counts["checksum_mismatched"],
        "files_checksum_unverified": counts["checksum_unverified"],
        "files_uploaded_missing_remote": counts["uploaded_missing_remote"],
        "files_uploaded_checksum_mismatch": counts["uploaded_checksum_mismatch"],
        "files_uploaded_remote_checksum_unavailable": counts["uploaded_remote_checksum_unavailable"],
        "all_compared_checksums_match": None if counts["checksum_verified"] == 0 else counts["checksum_mismatched"] == 0,
        "touched_partition_values": touched_partition_values,
        "sample_uris": [result.uri for result in results[:10]],
        "sample_results": [_sample_result(result) for result in results[:10]],
    }
    if logger is not None:
        logger.info(
            "Finished spec=%s uploaded=%s skipped_existing=%s checksum_matched=%s checksum_mismatched=%s planned=%s",
            spec.name,
            counts["uploaded"],
            counts["skipped_existing"],
            counts["checksum_matched"],
            counts["checksum_mismatched"],
            counts["planned"],
        )
    return summary, results, touched_partition_values


def publish_file_spec(
    *,
    spec: UploadSpec,
    config: GcpCloudConfig,
    skip_existing: bool,
    dry_run: bool,
) -> dict[str, object]:
    destination_blob_name = config.blob_path(*spec.destination_parts)
    existing_blob = None
    if skip_existing and not dry_run:
        existing_blob = list_blob_metadata(
            bucket_name=config.gcs_bucket,
            prefix=destination_blob_name,
            project_id=config.gcp_project_id,
        ).get(destination_blob_name)
    result = _upload_with_checksum_awareness(
        local_path=spec.local_path,
        config=config,
        destination_blob_name=destination_blob_name,
        dry_run=dry_run,
        existing_blob=existing_blob,
        local_md5_cache={},
    )
    return {
        "status": result.action,
        "local_path": str(spec.local_path),
        "gcs_destination": result.uri,
        "files_uploaded": 1 if result.action == "uploaded" else 0,
        "files_skipped_existing": 1 if result.action == "skipped_existing" else 0,
        "files_planned": 1 if result.action == "planned" else 0,
        "files_considered": 1,
        "checksum_aware": True,
        "checksum_verified": result.checksum_verified,
        "checksum_match": result.checksum_match,
        "upload_reason": result.upload_reason,
        "local_md5_base64": result.local_md5_base64,
        "remote_md5_base64": result.remote_md5_base64,
        "touched_partition_values": [],
        "sample_uris": [result.uri],
        "sample_results": [_sample_result(result)],
    }
