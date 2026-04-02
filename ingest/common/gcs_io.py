from __future__ import annotations

import base64
import hashlib
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from typing import Sequence
from urllib.parse import urlparse


@dataclass(frozen=True)
class GcsBlobMetadata:
    name: str
    uri: str
    md5_hash: str | None


@dataclass(frozen=True)
class GcsUploadResult:
    uri: str
    action: str
    checksum_verified: bool = False
    checksum_match: bool | None = None
    upload_reason: str | None = None
    local_md5_base64: str | None = None
    remote_md5_base64: str | None = None


def _blob_name(destination_prefix: str, relative_path: str) -> str:
    if destination_prefix:
        return str(PurePosixPath(destination_prefix) / PurePosixPath(relative_path))
    return str(PurePosixPath(relative_path))


def _storage_module():
    try:
        from google.cloud import storage
    except ModuleNotFoundError as exc:
        raise ModuleNotFoundError(
            "google-cloud-storage is required for GCS upload/list operations. "
            "Install it in the active environment before running this command."
        ) from exc
    return storage


def file_md5_base64(local_path: Path | str) -> str:
    local_path = Path(local_path)
    digest = hashlib.md5()
    with local_path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return base64.b64encode(digest.digest()).decode("ascii")


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
        return GcsUploadResult(uri=uri, action="planned", upload_reason="dry_run")

    storage = _storage_module()
    client = storage.Client(project=project_id)
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)
    if skip_if_exists and blob.exists():
        return GcsUploadResult(uri=uri, action="skipped_existing", upload_reason="skip_if_exists")

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

    storage = _storage_module() if not dry_run else None
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
    storage = _storage_module()
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


def list_blob_metadata(
    *,
    bucket_name: str,
    prefix: str,
    project_id: str,
    suffix: str | None = None,
) -> dict[str, GcsBlobMetadata]:
    storage = _storage_module()
    client = storage.Client(project=project_id)
    bucket = client.bucket(bucket_name)
    metadata: dict[str, GcsBlobMetadata] = {}
    for blob in bucket.list_blobs(prefix=prefix):
        if blob.name.endswith("/"):
            continue
        if suffix and not blob.name.endswith(suffix):
            continue
        metadata[blob.name] = GcsBlobMetadata(
            name=blob.name,
            uri=f"gs://{bucket_name}/{blob.name}",
            md5_hash=blob.md5_hash,
        )
    return metadata


def download_file(
    *,
    bucket_name: str,
    source_blob_name: str,
    destination_path: Path | str,
    project_id: str,
) -> Path:
    storage = _storage_module()
    destination_path = Path(destination_path)
    destination_path.parent.mkdir(parents=True, exist_ok=True)

    client = storage.Client(project=project_id)
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(source_blob_name)
    blob.download_to_filename(str(destination_path))
    return destination_path


def download_gcs_uri(
    *,
    uri: str,
    destination_path: Path | str,
    project_id: str,
) -> Path:
    parsed = urlparse(uri)
    if parsed.scheme != "gs" or not parsed.netloc or not parsed.path:
        raise ValueError(f"Expected a gs:// URI, got {uri!r}")

    return download_file(
        bucket_name=parsed.netloc,
        source_blob_name=parsed.path.lstrip("/"),
        destination_path=destination_path,
        project_id=project_id,
    )
