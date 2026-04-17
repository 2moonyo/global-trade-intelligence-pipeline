#!/usr/bin/env bash
set -euo pipefail

VM_REPO_DIR="${VM_REPO_DIR:-/var/lib/pipeline/capstone}"
ENV_FILE="${ENV_FILE:-/etc/capstone/pipeline.env}"
COMPOSE_FILE="${COMPOSE_FILE:-/var/lib/pipeline/capstone/docker/docker-compose.yml}"
PIPELINE_SERVICE="${PIPELINE_SERVICE:-pipeline}"
WAIT_ATTEMPTS="${WAIT_ATTEMPTS:-30}"
WAIT_SECONDS="${WAIT_SECONDS:-5}"
SYNC_SECRETS_BEFORE_RUN="${SYNC_SECRETS_BEFORE_RUN:-false}"
SECRET_PROJECT_ID="${SECRET_PROJECT_ID:-}"

declare -A SECRET_BY_KEY=(
  [FRED_API_KEY]="capstone-fred-api-key"
  [COMTRADE_API_KEY]="capstone-comtrade-api-key"
  [COMTRADE_API_KEY_DATA]="capstone-comtrade-api-key-data"
  [COMTRADE_API_KEY_DATA_A]="capstone-comtrade-api-key-data-a"
  [COMTRADE_API_KEY_DATA_B]="capstone-comtrade-api-key-data-b"
  [POSTGRES_USER]="capstone-postgres-user"
  [POSTGRES_PASSWORD]="capstone-postgres-password"
  [POSTGRES_DB]="capstone-postgres-db"
  [POSTGRES_SCHEMA]="capstone-postgres-schema"
)

SECRETS_ORDER=(
  FRED_API_KEY
  COMTRADE_API_KEY
  COMTRADE_API_KEY_DATA
  COMTRADE_API_KEY_DATA_A
  COMTRADE_API_KEY_DATA_B
  POSTGRES_USER
  POSTGRES_PASSWORD
  POSTGRES_DB
  POSTGRES_SCHEMA
)

compose() {
  sudo docker compose --env-file "${ENV_FILE}" -f "${COMPOSE_FILE}" "$@"
}

ensure_vm_repo() {
  if [[ ! -d "${VM_REPO_DIR}" ]]; then
    echo "Repo directory does not exist: ${VM_REPO_DIR}" >&2
    exit 1
  fi
  if [[ ! -f "${ENV_FILE}" ]]; then
    echo "Env file does not exist: ${ENV_FILE}" >&2
    exit 1
  fi
  if [[ ! -f "${COMPOSE_FILE}" ]]; then
    echo "Compose file does not exist: ${COMPOSE_FILE}" >&2
    exit 1
  fi
  cd "${VM_REPO_DIR}"
}

read_env_value() {
  local key="$1"
  python3 - "${ENV_FILE}" "${key}" <<'PY'
import pathlib
import sys

path = pathlib.Path(sys.argv[1])
key = sys.argv[2]
value = ""

if path.exists():
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        left, right = line.split("=", 1)
        if left.strip() != key:
            continue
        parsed = right.strip()
        if len(parsed) >= 2 and ((parsed[0] == "'" and parsed[-1] == "'") or (parsed[0] == '"' and parsed[-1] == '"')):
            parsed = parsed[1:-1]
        value = parsed

print(value)
PY
}

upsert_env_var() {
  local key="$1"
  local value="$2"
  python3 - "${ENV_FILE}" "${key}" "${value}" <<'PY'
import pathlib
import sys

path = pathlib.Path(sys.argv[1])
key = sys.argv[2]
value = sys.argv[3]

lines = []
if path.exists():
    lines = path.read_text(encoding="utf-8").splitlines()

prefix = f"{key}="
updated = False
new_lines = []
for line in lines:
    if line.startswith(prefix):
        new_lines.append(f"{key}={value}")
        updated = True
    else:
        new_lines.append(line)

if not updated:
    new_lines.append(f"{key}={value}")

path.write_text("\n".join(new_lines) + "\n", encoding="utf-8")
PY
}

resolve_secret_project_id() {
  if [[ -n "${SECRET_PROJECT_ID}" ]]; then
    echo "${SECRET_PROJECT_ID}"
    return 0
  fi

  local from_env
  from_env="$(read_env_value "GCP_PROJECT_ID")"
  if [[ -n "${from_env}" ]]; then
    echo "${from_env}"
    return 0
  fi

  if command -v gcloud >/dev/null 2>&1; then
    gcloud config get-value project 2>/dev/null || true
    return 0
  fi

  echo ""
}

sync_env_from_secret_manager() {
  if [[ "${SYNC_SECRETS_BEFORE_RUN}" != "true" ]]; then
    return 0
  fi

  if ! command -v gcloud >/dev/null 2>&1; then
    echo "gcloud is required for secret sync but was not found." >&2
    exit 1
  fi

  local project_id
  project_id="$(resolve_secret_project_id)"
  if [[ -z "${project_id}" ]]; then
    echo "Could not resolve project id for secret sync. Set SECRET_PROJECT_ID or GCP_PROJECT_ID." >&2
    exit 1
  fi

  echo "Syncing selected runtime secrets from project ${project_id} into ${ENV_FILE}"
  set +x
  local key
  for key in "${SECRETS_ORDER[@]}"; do
    local secret_id
    local secret_value
    secret_id="${SECRET_BY_KEY[${key}]}"
    if secret_value="$(gcloud secrets versions access latest --secret="${secret_id}" --project="${project_id}" 2>/dev/null)" && [[ -n "${secret_value}" ]]; then
      upsert_env_var "${key}" "${secret_value}"
      echo "- updated ${key} from ${secret_id}"
    else
      echo "- skipped ${key}; secret ${secret_id} unavailable"
    fi
  done
  set -x
}

ensure_stack() {
  compose up -d
  local attempt
  for attempt in $(seq 1 "${WAIT_ATTEMPTS}"); do
    if compose exec -T "${PIPELINE_SERVICE}" true >/dev/null 2>&1; then
      return 0
    fi
    echo "Waiting for ${PIPELINE_SERVICE} container (${attempt}/${WAIT_ATTEMPTS})..."
    sleep "${WAIT_SECONDS}"
  done
  echo "Container ${PIPELINE_SERVICE} did not become ready in time." >&2
  exit 1
}

ops_init_all() {
  compose exec -T "${PIPELINE_SERVICE}" scripts/run_pipeline.sh ops-init-all
}

run_dataset_batch() {
  local dataset_name="$1"
  local batch_id="$2"
  shift 2

  compose exec -T "${PIPELINE_SERVICE}" \
    scripts/run_pipeline.sh dataset-batch "${dataset_name}" "${batch_id}" "$@"
}

prepare_runtime() {
  ensure_vm_repo
  sync_env_from_secret_manager
  ensure_stack
}
