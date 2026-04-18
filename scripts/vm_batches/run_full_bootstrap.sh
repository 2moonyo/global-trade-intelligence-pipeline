#!/usr/bin/env bash
set -euo pipefail

# Run the complete first-time VM bootstrap sequence in the safest dependency order.
# Intended to run on the VM host after `make vm-bootstrap` has completed locally.

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
VM_REPO_DIR="${VM_REPO_DIR:-/var/lib/pipeline/capstone}"
ENV_FILE="${ENV_FILE:-/etc/capstone/pipeline.env}"
COMPOSE_FILE="${COMPOSE_FILE:-/var/lib/pipeline/capstone/docker/docker-compose.yml}"

run_set() {
  local set_name="$1"
  shift

  echo ""
  echo "================================================================"
  echo "Running VM batch set: ${set_name}"
  echo "================================================================"
  "${SCRIPT_DIR}/run_set.sh" "${set_name}" "$@"
}

run_country_trade_and_energy() {
  echo ""
  echo "================================================================"
  echo "Running dependent World Bank energy lane"
  echo "================================================================"
  sudo docker compose --env-file "${ENV_FILE}" -f "${COMPOSE_FILE}" \
    exec -T pipeline scripts/run_pipeline.sh country-trade-and-energy
}

if [[ ! -d "${VM_REPO_DIR}" ]]; then
  echo "Repo directory does not exist: ${VM_REPO_DIR}" >&2
  exit 1
fi

cd "${VM_REPO_DIR}"

run_set noncomtrade-phase-1-all
run_set noncomtrade-phase-2-all

run_set comtrade-day-1
run_set comtrade-day-2
run_set comtrade-day-3
run_set comtrade-day-4
run_set comtrade-day-5
run_set comtrade-day-6

run_country_trade_and_energy

echo ""
echo "Full VM bootstrap sequence completed successfully."
