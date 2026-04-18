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

compose() {
  sudo docker compose --env-file "${ENV_FILE}" -f "${COMPOSE_FILE}" "$@"
}

ensure_vm_repo() {
  if [[ ! -d "${VM_REPO_DIR}" ]]; then
    echo "Repo directory does not exist: ${VM_REPO_DIR}" >&2
    exit 1
  fi
  # /etc/capstone is intentionally root-protected because pipeline.env contains
  # secrets. Check it through sudo, then let sudo docker compose read it.
  if ! sudo test -f "${ENV_FILE}"; then
    echo "Env file does not exist: ${ENV_FILE}" >&2
    exit 1
  fi
  if [[ ! -f "${COMPOSE_FILE}" ]]; then
    echo "Compose file does not exist: ${COMPOSE_FILE}" >&2
    exit 1
  fi
  cd "${VM_REPO_DIR}"
}

sync_env_from_secret_manager() {
  if [[ "${SYNC_SECRETS_BEFORE_RUN}" != "true" ]]; then
    return 0
  fi

  local render_script="${VM_REPO_DIR}/scripts/render_pipeline_env_from_secret_manager.sh"
  if [[ ! -x "${render_script}" ]]; then
    echo "Secret Manager env renderer is missing or not executable: ${render_script}" >&2
    exit 1
  fi

  local args=(
    --output-file "${ENV_FILE}"
    --base-env-file "${ENV_FILE}"
  )
  if [[ -n "${SECRET_PROJECT_ID}" ]]; then
    args+=(--project "${SECRET_PROJECT_ID}")
  fi

  "${render_script}" "${args[@]}"
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
