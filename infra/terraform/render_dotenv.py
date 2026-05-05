from __future__ import annotations

import argparse
import json
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[2]
TFVARS_PATH = PROJECT_ROOT / "infra" / "terraform" / "terraform.tfvars.json"


def _load_tfvars(tfvars_path: Path) -> dict[str, object]:
    if not tfvars_path.exists():
        raise FileNotFoundError(
            f"Missing {tfvars_path}. Copy terraform.tfvars.json.example and fill in your values first."
        )
    with tfvars_path.open("r", encoding="utf-8") as handle:
        payload = json.load(handle)
    if not isinstance(payload, dict):
        raise RuntimeError(f"Expected a JSON object in {tfvars_path}")
    return payload


def _build_env_map(tfvars: dict[str, object], profile: str) -> dict[str, object]:
    env_map: dict[str, object] = {
        "GCP_PROJECT_ID": tfvars.get("project_id", ""),
        "GCP_LOCATION": tfvars.get("gcp_location", "us-central1"),
        "GCS_BUCKET": tfvars.get("gcs_bucket_name", ""),
        "GCS_PREFIX": tfvars.get("gcs_prefix", ""),
        "GCP_BIGQUERY_RAW_DATASET": tfvars.get("raw_dataset_id", "raw"),
        "GCP_BIGQUERY_ANALYTICS_DATASET": tfvars.get("analytics_dataset_id", "analytics"),
        "DBT_BIGQUERY_DATASET": tfvars.get("analytics_dataset_id", "analytics"),
    }
    if profile == "vm":
        env_map.update(
            {
                "CAPSTONE_CONTAINER_ENV_FILE": tfvars.get("vm_env_file_path", "/etc/capstone/pipeline.env"),
                "GOOGLE_AUTH_MODE": "vm_metadata",
                "GOOGLE_APPLICATION_CREDENTIALS": "",
                "POSTGRES_SCHEMA": "ops",
                "BATCH_PLAN_PATH": "ops/batch_plan.json",
                "EXECUTION_PROFILE": tfvars.get("execution_profile", "all_vm"),
                "EXECUTION_RUNTIME": "vm",
                "EXECUTION_PROFILE_PATH": "ops/execution_profiles.json",
                "OPS_POSTGRES_ENABLED": "true",
                "ENABLE_BIGQUERY_OPS_MIRROR": "true",
                "OPS_STRICT_BIGQUERY_MIRROR": "false",
                "AWS_EC2_METADATA_DISABLED": "true",
                "TELEMETRY_OPTOUT": "true",
            }
        )
    return env_map


def _build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Render environment variables from Terraform tfvars.")
    parser.add_argument(
        "--tfvars-file",
        default=str(TFVARS_PATH),
        help="Path to terraform.tfvars.json.",
    )
    parser.add_argument(
        "--format",
        choices=("dotenv", "export"),
        default="dotenv",
        help="Emit KEY=value lines or shell export statements.",
    )
    parser.add_argument(
        "--profile",
        choices=("base", "vm"),
        default="base",
        help="Emit the base cloud env only, or the VM runtime env overlay as well.",
    )
    return parser


def main() -> None:
    parser = _build_arg_parser()
    args = parser.parse_args()

    tfvars_path = Path(args.tfvars_file).expanduser()
    tfvars = _load_tfvars(tfvars_path)
    env_map = _build_env_map(tfvars, profile=args.profile)
    for key, value in env_map.items():
        if args.format == "export":
            print(f"export {key}={json.dumps(str(value))}")
        else:
            print(f"{key}={value}")


if __name__ == "__main__":
    main()
