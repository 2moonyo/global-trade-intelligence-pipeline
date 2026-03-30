from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from typing import Sequence

from google.cloud import storage


@dataclass(frozen=True)
class GcsUploadResult:
    uri: str
    action: str


def _blob_name(destination_prefix: str, relative_path: str) -> str:
    if destination_prefix:
        return str(PurePosixPath(destination_prefix) / PurePosixPath(relative_path))
    return str(PurePosixPath(relative_path))


def upload_file(
    local_path: Path | str,
    *,
    bucket_name: str,
    destination_blob_name: str,
    project_id: str,
    skip_if_exists: bool = False,
    dry_run: bool = False,
) -> GcsUploadResult:
    local_path = Path(local_path)
    if not local_path.exists() or not local_path.is_file():
        raise FileNotFoundError(f"Expected file at {local_path}")

    uri = f"gs://{bucket_name}/{destination_blob_name}"
    if dry_run:
        return GcsUploadResult(uri=uri, action="planned")

    client = storage.Client(project=project_id)
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)
    if skip_if_exists and blob.exists():
        return GcsUploadResult(uri=uri, action="skipped_existing")

    blob.upload_from_filename(str(local_path))
    return GcsUploadResult(uri=uri, action="uploaded")


def upload_tree(
    local_root: Path | str,
    *,
    bucket_name: str,
    destination_prefix: str,
    project_id: str,
    include_suffixes: Sequence[str] | None = None,
    skip_if_exists: bool = False,
    dry_run: bool = False,
) -> list[GcsUploadResult]:
    local_root = Path(local_root)
    if not local_root.exists() or not local_root.is_dir():
        raise FileNotFoundError(f"Expected directory at {local_root}")

    files = sorted(path for path in local_root.rglob("*") if path.is_file())
    if include_suffixes:
        files = [path for path in files if path.suffix in set(include_suffixes)]

    if not files:
        return []

    client = None if dry_run else storage.Client(project=project_id)
    bucket = None if dry_run else client.bucket(bucket_name)

    uploaded: list[GcsUploadResult] = []
    for path in files:
        relative_path = path.relative_to(local_root).as_posix()
        blob_name = _blob_name(destination_prefix, relative_path)
        uri = f"gs://{bucket_name}/{blob_name}"
        if dry_run:
            uploaded.append(GcsUploadResult(uri=uri, action="planned"))
            continue

        blob = bucket.blob(blob_name)
        if skip_if_exists and blob.exists():
            uploaded.append(GcsUploadResult(uri=uri, action="skipped_existing"))
            continue

        blob.upload_from_filename(str(path))
        uploaded.append(GcsUploadResult(uri=uri, action="uploaded"))
    return uploaded


def list_blob_uris(
    *,
    bucket_name: str,
    prefix: str,
    project_id: str,
    suffix: str | None = None,
) -> list[str]:
    client = storage.Client(project=project_id)
    bucket = client.bucket(bucket_name)
    uris: list[str] = []
    for blob in bucket.list_blobs(prefix=prefix):
        if blob.name.endswith("/"):
            continue
        if suffix and not blob.name.endswith(suffix):
            continue
        uris.append(f"gs://{bucket_name}/{blob.name}")
    return sorted(uris)
