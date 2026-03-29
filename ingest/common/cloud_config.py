from __future__ import annotations

import json
import os
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from typing import Iterable

from dotenv import load_dotenv


load_dotenv()
PROJECT_ROOT = Path(__file__).resolve().parents[2]


def _load_terraform_settings() -> dict[str, object]:
    candidate_paths = (
        PROJECT_ROOT / "infra" / "terraform" / "terraform.tfvars.json",
        PROJECT_ROOT / "infra" / "terraform" / "terraform.auto.tfvars.json",
        PROJECT_ROOT / "infra" / "terraform" / "local.auto.tfvars.json",
    )
    for path in candidate_paths:
        if not path.exists():
            continue
        with path.open("r", encoding="utf-8") as handle:
            payload = json.load(handle)
        if isinstance(payload, dict):
            return payload
    return {}


TERRAFORM_SETTINGS = _load_terraform_settings()


def _require_env(name: str) -> str:
    value = os.getenv(name)
    if value and value.strip():
        return value.strip()
    tf_value = TERRAFORM_SETTINGS.get(name.lower()) or TERRAFORM_SETTINGS.get(name)
    if isinstance(tf_value, str) and tf_value.strip():
        return tf_value.strip()
    raise RuntimeError(
        f"Missing required environment variable {name}. "
        "Populate it in your shell, in a local .env file, or in infra/terraform/terraform.tfvars.json."
    )


def _get_setting(
    *,
    env_name: str,
    terraform_keys: tuple[str, ...] = (),
    default: str | None = None,
    required: bool = False,
) -> str:
    value = os.getenv(env_name)
    if value and value.strip():
        return value.strip()

    for key in terraform_keys:
        tf_value = TERRAFORM_SETTINGS.get(key)
        if isinstance(tf_value, str) and tf_value.strip():
            return tf_value.strip()

    if default is not None:
        return default

    if required:
        raise RuntimeError(
            f"Missing required setting {env_name}. "
            "Provide it with an environment variable, .env file, or infra/terraform/terraform.tfvars.json."
        )

    return ""


def _clean_bucket_name(value: str) -> str:
    bucket = value.strip()
    if bucket.startswith("gs://"):
        bucket = bucket[5:]
    return bucket.strip("/")


def _clean_path_part(value: object) -> str:
    text = str(value).strip().strip("/")
    return text


@dataclass(frozen=True)
class GcpCloudConfig:
    gcp_project_id: str
    gcp_location: str
    gcs_bucket: str
    gcs_prefix: str = ""
    bq_raw_dataset: str = "raw"
    bq_analytics_dataset: str = "analytics"

    @classmethod
    def from_env(cls) -> "GcpCloudConfig":
        return cls(
            gcp_project_id=_get_setting(
                env_name="GCP_PROJECT_ID",
                terraform_keys=("project_id",),
                required=True,
            ),
            gcp_location=_get_setting(
                env_name="GCP_LOCATION",
                terraform_keys=("gcp_location",),
                default="EU",
            ),
            gcs_bucket=_clean_bucket_name(
                _get_setting(
                    env_name="GCS_BUCKET",
                    terraform_keys=("gcs_bucket_name", "bucket_name"),
                    required=True,
                )
            ),
            gcs_prefix=_clean_path_part(
                _get_setting(
                    env_name="GCS_PREFIX",
                    terraform_keys=("gcs_prefix",),
                    default="",
                )
            ),
            bq_raw_dataset=_get_setting(
                env_name="GCP_BIGQUERY_RAW_DATASET",
                terraform_keys=("raw_dataset_id", "gcp_bigquery_raw_dataset"),
                default="raw",
            ),
            bq_analytics_dataset=_get_setting(
                env_name="GCP_BIGQUERY_ANALYTICS_DATASET",
                terraform_keys=("analytics_dataset_id", "gcp_bigquery_analytics_dataset"),
                default="analytics",
            ),
        )

    def blob_path(self, *parts: object) -> str:
        clean_parts: list[str] = []
        if self.gcs_prefix:
            clean_parts.append(self.gcs_prefix)
        clean_parts.extend(_clean_path_part(part) for part in parts if _clean_path_part(part))
        if not clean_parts:
            return ""
        return str(PurePosixPath(*clean_parts))

    def gcs_uri(self, *parts: object) -> str:
        blob_path = self.blob_path(*parts)
        if not blob_path:
            return f"gs://{self.gcs_bucket}"
        return f"gs://{self.gcs_bucket}/{blob_path}"

    def join_relative_parts(self, *parts: object) -> str:
        clean_parts = [_clean_path_part(part) for part in parts if _clean_path_part(part)]
        return str(PurePosixPath(*clean_parts))

    def iter_gcs_uris(self, prefixes: Iterable[object]) -> list[str]:
        return [self.gcs_uri(prefix) for prefix in prefixes]
