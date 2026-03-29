from __future__ import annotations

import argparse
import json
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[2]
TFVARS_PATH = PROJECT_ROOT / "infra" / "terraform" / "terraform.tfvars.json"


def _load_tfvars() -> dict[str, object]:
    if not TFVARS_PATH.exists():
        raise FileNotFoundError(
            f"Missing {TFVARS_PATH}. Copy terraform.tfvars.json.example and fill in your values first."
        )
    with TFVARS_PATH.open("r", encoding="utf-8") as handle:
        payload = json.load(handle)
    if not isinstance(payload, dict):
        raise RuntimeError(f"Expected a JSON object in {TFVARS_PATH}")
    return payload


def _build_env_map(tfvars: dict[str, object]) -> dict[str, object]:
    return {
        "GCP_PROJECT_ID": tfvars.get("project_id", ""),
        "GCP_LOCATION": tfvars.get("gcp_location", "EU"),
        "GCS_BUCKET": tfvars.get("gcs_bucket_name", ""),
        "GCS_PREFIX": tfvars.get("gcs_prefix", ""),
        "GCP_BIGQUERY_RAW_DATASET": tfvars.get("raw_dataset_id", "raw"),
        "GCP_BIGQUERY_ANALYTICS_DATASET": tfvars.get("analytics_dataset_id", "analytics"),
        "DBT_BIGQUERY_DATASET": tfvars.get("analytics_dataset_id", "analytics"),
    }


def _build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Render environment variables from Terraform tfvars.")
    parser.add_argument(
        "--format",
        choices=("dotenv", "export"),
        default="dotenv",
        help="Emit KEY=value lines or shell export statements.",
    )
    return parser


def main() -> None:
    parser = _build_arg_parser()
    args = parser.parse_args()

    tfvars = _load_tfvars()
    env_map = _build_env_map(tfvars)
    for key, value in env_map.items():
        if args.format == "export":
            print(f"export {key}={json.dumps(str(value))}")
        else:
            print(f"{key}={value}")


if __name__ == "__main__":
    main()
