#!/usr/bin/env bash
set -euo pipefail

# Fast-track bootstrap sequence via Bruin dataset batch wrapper in orchestrator.
# Intended to run on the VM host.

VM_REPO_DIR="${VM_REPO_DIR:-/var/lib/pipeline/capstone}"
ENV_FILE="${ENV_FILE:-/etc/capstone/pipeline.env}"
COMPOSE_FILE="${COMPOSE_FILE:-docker/docker-compose.yml}"
PLAN_PATH="${PLAN_PATH:-ops/batch_plan.json}"
ORCHESTRATOR_SERVICE="${ORCHESTRATOR_SERVICE:-orchestrator}"
PIPELINE_SERVICE="${PIPELINE_SERVICE:-pipeline}"
WAIT_ATTEMPTS="${WAIT_ATTEMPTS:-30}"
WAIT_SECONDS="${WAIT_SECONDS:-5}"

if [[ ! -d "${VM_REPO_DIR}" ]]; then
  echo "Repo directory does not exist: ${VM_REPO_DIR}" >&2
  exit 1
fi

if [[ ! -f "${VM_REPO_DIR}/${COMPOSE_FILE}" ]]; then
  echo "Compose file not found: ${VM_REPO_DIR}/${COMPOSE_FILE}" >&2
  exit 1
fi

if [[ ! -f "${ENV_FILE}" ]]; then
  echo "Env file not found: ${ENV_FILE}" >&2
  exit 1
fi

cd "${VM_REPO_DIR}"

compose() {
  sudo docker compose --env-file "${ENV_FILE}" -f "${COMPOSE_FILE}" "$@"
}

wait_for_orchestrator() {
  local attempt
  for attempt in $(seq 1 "${WAIT_ATTEMPTS}"); do
    if compose exec -T "${ORCHESTRATOR_SERVICE}" true >/dev/null 2>&1; then
      echo "Orchestrator container is ready."
      return 0
    fi
    echo "Waiting for orchestrator container (${attempt}/${WAIT_ATTEMPTS})..."
    sleep "${WAIT_SECONDS}"
  done

  echo "Orchestrator did not become ready in time." >&2
  return 1
}

run_batch() {
  local dataset_name="$1"
  local batch_id="$2"

  echo ""
  echo "================================================================"
  echo "Running batch: ${batch_id} (dataset: ${dataset_name})"
  echo "================================================================"

  compose exec -T \
    -e DATASET_NAME="${dataset_name}" \
    -e BATCH_ID="${batch_id}" \
    -e BATCH_PLAN_PATH="${PLAN_PATH}" \
    "${ORCHESTRATOR_SERVICE}" \
    python bruin/pipelines/dataset_batch/assets/run_dataset_batch.py
}

echo "Starting stack and validating containers..."
compose up -d
wait_for_orchestrator

echo "Initializing ops stores (Postgres + BigQuery raw ops tables)..."
compose exec -T "${PIPELINE_SERVICE}" scripts/run_pipeline.sh ops-init-all

# Requested fast-track order:
# 1) Comtrade day 1
# 2) World Bank energy
# 3) Comtrade day 2
# 4) Comtrade day 3
# 5) Non-Comtrade bootstrap phase 2 batches
run_batch comtrade comtrade_bootstrap_day_1
run_batch worldbank_energy worldbank_energy_bootstrap_full
run_batch comtrade comtrade_bootstrap_day_2
run_batch comtrade comtrade_bootstrap_day_3
run_batch portwatch portwatch_bootstrap_phase_2
run_batch brent brent_bootstrap_phase_2
run_batch fx fx_bootstrap_phase_2
run_batch events events_bootstrap_phase_2

echo ""
echo "Fast-track Bruin bootstrap sequence completed successfully."
