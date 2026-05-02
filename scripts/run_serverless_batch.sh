#!/usr/bin/env bash
set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${PROJECT_ROOT}"

DATASET_NAME="${1:-}"
BATCH_ID="${2:-}"
if [[ -z "${DATASET_NAME}" || -z "${BATCH_ID}" ]]; then
  cat >&2 <<'USAGE'
Usage: scripts/run_serverless_batch.sh <dataset_name> <batch_id> [dataset-batch args...]
USAGE
  exit 2
fi
shift 2

CLI_EXECUTION_PROFILE=""
CLI_TARGET_RUNTIME=""
CLI_BRUIN_ENVIRONMENT=""
CLI_BRUIN_PIPELINE_PATH=""
PIPELINE_EXTRA_ARGS=()

while [[ $# -gt 0 ]]; do
  case "$1" in
    --execution-profile)
      CLI_EXECUTION_PROFILE="${2:-}"
      shift 2
      ;;
    --target-runtime)
      CLI_TARGET_RUNTIME="${2:-}"
      shift 2
      ;;
    --bruin-environment)
      CLI_BRUIN_ENVIRONMENT="${2:-}"
      shift 2
      ;;
    --bruin-pipeline-path)
      CLI_BRUIN_PIPELINE_PATH="${2:-}"
      shift 2
      ;;
    *)
      PIPELINE_EXTRA_ARGS+=("$1")
      shift
      ;;
  esac
done

export EXECUTION_PROFILE="${EXECUTION_PROFILE:-hybrid_vm_serverless}"
export EXECUTION_RUNTIME="${EXECUTION_RUNTIME:-cloud_run}"
export EXECUTION_PROFILE_PATH="${EXECUTION_PROFILE_PATH:-ops/execution_profiles.json}"
export OPS_POSTGRES_ENABLED="${OPS_POSTGRES_ENABLED:-false}"
export ENABLE_BIGQUERY_OPS_MIRROR="${ENABLE_BIGQUERY_OPS_MIRROR:-true}"
export OPS_STRICT_BIGQUERY_MIRROR="${OPS_STRICT_BIGQUERY_MIRROR:-false}"
export GOOGLE_AUTH_MODE="${GOOGLE_AUTH_MODE:-auto}"
export BATCH_PLAN_PATH="${BATCH_PLAN_PATH:-ops/batch_plan.json}"
export AWS_EC2_METADATA_DISABLED="${AWS_EC2_METADATA_DISABLED:-true}"
export TELEMETRY_OPTOUT="${TELEMETRY_OPTOUT:-true}"
export PYTHONPATH="${PROJECT_ROOT}:${PYTHONPATH:-}"
export UV_CACHE_DIR="${PROJECT_ROOT}/.uv-cache"
export BRUIN_ENVIRONMENT="${BRUIN_ENVIRONMENT:-production}"
export SERVERLESS_RUN_ID="${SERVERLESS_RUN_ID:-serverless_$(date -u +%Y%m%dT%H%M%SZ)_${DATASET_NAME}_${BATCH_ID}}"

if [[ -n "${CLI_EXECUTION_PROFILE}" ]]; then
  export EXECUTION_PROFILE="${CLI_EXECUTION_PROFILE}"
fi
if [[ -n "${CLI_TARGET_RUNTIME}" ]]; then
  export TARGET_RUNTIME="${CLI_TARGET_RUNTIME}"
else
  export TARGET_RUNTIME="${TARGET_RUNTIME:-cloud_run}"
fi
if [[ -n "${CLI_BRUIN_ENVIRONMENT}" ]]; then
  export BRUIN_ENVIRONMENT="${CLI_BRUIN_ENVIRONMENT}"
fi
if [[ -n "${CLI_BRUIN_PIPELINE_PATH}" ]]; then
  export BRUIN_PIPELINE_PATH="${CLI_BRUIN_PIPELINE_PATH}"
fi

mkdir -p "${PROJECT_ROOT}/.uv-cache" "${PROJECT_ROOT}/data" "${PROJECT_ROOT}/logs" "${PROJECT_ROOT}/target" "${PROJECT_ROOT}/dbt_packages"

source "${PROJECT_ROOT}/scripts/google_auth_env.sh"
configure_google_auth --quiet

set +e
python warehouse/serverless_preflight.py --dataset-name "${DATASET_NAME}" --batch-id "${BATCH_ID}"
preflight_status=$?
if [[ "${preflight_status}" -eq 0 ]]; then
  if [[ -n "${BRUIN_PIPELINE_PATH:-}" ]]; then
    git init "${PROJECT_ROOT}" >/dev/null 2>&1 || true
    bruin run \
      --environment "${BRUIN_ENVIRONMENT}" \
      --force \
      "${BRUIN_PIPELINE_PATH}"
    batch_status=$?
  else
    scripts/run_pipeline.sh dataset-batch \
      "${DATASET_NAME}" \
      "${BATCH_ID}" \
      --plan-path "${BATCH_PLAN_PATH}" \
      --trigger-type cloud_run \
      --bruin-pipeline-name "cloud_run.${DATASET_NAME}.${BATCH_ID}" \
      "${PIPELINE_EXTRA_ARGS[@]}"
    batch_status=$?
  fi
else
  batch_status="${preflight_status}"
fi
set -e

python warehouse/upload_serverless_artifacts.py \
  --dataset-name "${DATASET_NAME}" \
  --batch-id "${BATCH_ID}" \
  --run-label "${SERVERLESS_RUN_ID}" \
  --status "${batch_status}" || true

exit "${batch_status}"
