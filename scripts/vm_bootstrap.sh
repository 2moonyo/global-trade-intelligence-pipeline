#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
TF_DIR="${PROJECT_ROOT}/infra/terraform"
TFVARS_PATH="${TF_DIR}/terraform.tfvars.json"
ENV_FILE="${PROJECT_ROOT}/.env"

DEFAULT_BRANCH="$(git -C "${PROJECT_ROOT}" branch --show-current 2>/dev/null || true)"
DEFAULT_BRANCH="${DEFAULT_BRANCH:-cloud_migration}"

TRANSFER_METHOD="copy"
BRANCH="${DEFAULT_BRANCH}"
REPO_URL=""
SSH_KEY_PATH="${HOME}/.ssh/google_compute_engine"
VM_REPO_DIR=""
VM_NAME=""
VM_ZONE=""
PROJECT_ID=""
VM_ENV_FILE_PATH=""
SKIP_TERRAFORM=0
SKIP_SECRET_SYNC=0
SKIP_STACK_START=0
SHOW_RESOLVED=0
RESET_KNOWN_HOST=0
TERRAFORM_VAR_ARGS=()

usage() {
  cat <<'EOF'
Bootstrap the VM runtime for this repo from your laptop.

This helper:
1. Applies Terraform for the current tfvars
2. Pushes approved secrets from .env to Secret Manager
3. Resolves VM external IP and Linux user automatically
4. Syncs repo contents to the VM (default: local copy, no GitHub access required on VM)
5. Renders /etc/capstone/pipeline.env on the VM from tfvars + Secret Manager
6. Starts the capstone stack

Usage:
  scripts/vm_bootstrap.sh [options]

Options:
  --tfvars-path PATH             Terraform tfvars file (default: infra/terraform/terraform.tfvars.json)
  --env-file PATH                Local env file used for Secret Manager sync (default: .env)
  --ssh-key-path PATH            Local private SSH key path (default: ~/.ssh/google_compute_engine)
  --vm-repo-dir PATH             Repo path on VM (default: tfvars vm_repo_root or /var/lib/pipeline/capstone)
  --vm-name NAME                 Override primary VM name from tfvars
  --vm-zone ZONE                 Override primary VM zone from tfvars
  --project PROJECT_ID           Override project id from tfvars
  --branch NAME                  Branch name to use when transfer method is git (default: current local branch)
  --repo-url URL                 Git remote URL for git transfer mode (default: local origin URL)
  --transfer METHOD              Repo transfer method: copy or git (default: copy)
  --reset-known-host             Remove stale SSH known_hosts entries for the resolved VM host before connecting
  --skip-terraform               Skip terraform init/apply
  --skip-secret-sync             Skip syncing local .env secrets to Secret Manager
  --skip-stack-start             Skip rendering env + starting stack on the VM
  --show-resolved                Print resolved VM/project values before execution
  -h, --help                     Show this help
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --tfvars-path)
      TFVARS_PATH="$2"
      shift 2
      ;;
    --env-file)
      ENV_FILE="$2"
      shift 2
      ;;
    --ssh-key-path)
      SSH_KEY_PATH="$2"
      shift 2
      ;;
    --vm-repo-dir)
      VM_REPO_DIR="$2"
      shift 2
      ;;
    --vm-name)
      VM_NAME="$2"
      shift 2
      ;;
    --vm-zone)
      VM_ZONE="$2"
      shift 2
      ;;
    --project)
      PROJECT_ID="$2"
      shift 2
      ;;
    --branch)
      BRANCH="$2"
      shift 2
      ;;
    --repo-url)
      REPO_URL="$2"
      shift 2
      ;;
    --transfer)
      TRANSFER_METHOD="$2"
      shift 2
      ;;
    --reset-known-host)
      RESET_KNOWN_HOST=1
      shift
      ;;
    --skip-terraform)
      SKIP_TERRAFORM=1
      shift
      ;;
    --skip-secret-sync)
      SKIP_SECRET_SYNC=1
      shift
      ;;
    --skip-stack-start)
      SKIP_STACK_START=1
      shift
      ;;
    --show-resolved)
      SHOW_RESOLVED=1
      shift
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "Unknown argument: $1" >&2
      usage
      exit 2
      ;;
  esac
done

TFVARS_PATH="$(python3 -c 'import pathlib,sys; print(pathlib.Path(sys.argv[1]).expanduser().resolve())' "${TFVARS_PATH}")"
ENV_FILE="$(python3 -c 'import pathlib,sys; print(pathlib.Path(sys.argv[1]).expanduser().resolve())' "${ENV_FILE}")"

case "${TRANSFER_METHOD}" in
  copy|git)
    ;;
  *)
    echo "Unsupported transfer method: ${TRANSFER_METHOD}. Use copy or git." >&2
    exit 2
    ;;
esac

require_command() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "Required command not found in PATH: $1" >&2
    exit 1
  fi
}

read_tfvar() {
  python3 - "${TFVARS_PATH}" "$1" "$2" <<'PY'
import json
import pathlib
import sys

tfvars_path = pathlib.Path(sys.argv[1])
key = sys.argv[2]
default = sys.argv[3]

payload = json.loads(tfvars_path.read_text(encoding="utf-8"))
value = payload.get(key, default)

if value is None:
    value = ""
elif isinstance(value, bool):
    value = "true" if value else "false"
elif isinstance(value, (dict, list)):
    value = json.dumps(value)

print(value)
PY
}

ensure_adc() {
  if gcloud auth application-default print-access-token >/dev/null 2>&1; then
    return 0
  fi

  echo "Application Default Credentials not found. Launching 'gcloud auth application-default login'..."
  gcloud auth application-default login
}

resolve_host_from_terraform_or_gcloud() {
  local host

  host="$(terraform -chdir="${TF_DIR}" output -raw primary_vm_external_ip 2>/dev/null || true)"
  if [[ -n "${host}" && "${host}" != "null" ]]; then
    printf '%s\n' "${host}"
    return 0
  fi

  gcloud compute instances describe "${VM_NAME}" \
    --project "${PROJECT_ID}" \
    --zone "${VM_ZONE}" \
    --format='get(networkInterfaces[0].accessConfigs[0].natIP)'
}

ensure_local_ssh_key() {
  if [[ -f "${SSH_KEY_PATH}" ]]; then
    return 0
  fi

  echo "Local SSH key not found at ${SSH_KEY_PATH}. Bootstrapping it with gcloud compute ssh..."
  gcloud compute ssh "${VM_NAME}" \
    --project "${PROJECT_ID}" \
    --zone "${VM_ZONE}" \
    --command 'echo ssh-bootstrap-ok' >/dev/null

  if [[ ! -f "${SSH_KEY_PATH}" ]]; then
    echo "Expected SSH key was not created at ${SSH_KEY_PATH}." >&2
    exit 1
  fi
}

resolve_vm_user() {
  gcloud compute ssh "${VM_NAME}" \
    --project "${PROJECT_ID}" \
    --zone "${VM_ZONE}" \
    --command 'whoami' 2>/dev/null | tail -n 1 | tr -d '\r'
}

reset_known_host() {
  local host="$1"

  if ! command -v ssh-keygen >/dev/null 2>&1; then
    echo "Warning: ssh-keygen is not available; cannot reset known_hosts entry for ${host}." >&2
    return 0
  fi

  ssh-keygen -R "${host}" >/dev/null 2>&1 || true
  ssh-keygen -R "[${host}]:22" >/dev/null 2>&1 || true
}

PROJECT_ID="${PROJECT_ID:-$(read_tfvar "project_id" "")}"
VM_NAME="${VM_NAME:-$(read_tfvar "primary_vm_name" "capstone-vm-eu")}"
VM_ZONE="${VM_ZONE:-$(read_tfvar "primary_zone" "europe-west1-b")}"
VM_REPO_DIR="${VM_REPO_DIR:-$(read_tfvar "vm_repo_root" "/var/lib/pipeline/capstone")}"
VM_ENV_FILE_PATH="${VM_ENV_FILE_PATH:-$(read_tfvar "vm_env_file_path" "/etc/capstone/pipeline.env")}"

if [[ -z "${PROJECT_ID}" ]]; then
  echo "Could not resolve project_id from ${TFVARS_PATH}. Pass --project explicitly." >&2
  exit 1
fi

if [[ ! -f "${TFVARS_PATH}" ]]; then
  echo "Terraform tfvars file not found: ${TFVARS_PATH}" >&2
  exit 1
fi

if [[ ! -f "${ENV_FILE}" && "${SKIP_SECRET_SYNC}" -eq 0 ]]; then
  echo "Local env file not found: ${ENV_FILE}" >&2
  exit 1
fi

require_command gcloud
require_command terraform
require_command python3
require_command ssh
require_command scp
require_command tar

if [[ "${TFVARS_PATH}" != "${TF_DIR}/terraform.tfvars.json" ]]; then
  TERRAFORM_VAR_ARGS+=("-var-file=${TFVARS_PATH}")
fi

ensure_adc
gcloud auth application-default set-quota-project "${PROJECT_ID}" >/dev/null 2>&1 || true

if [[ "${SKIP_TERRAFORM}" -eq 0 ]]; then
  terraform -chdir="${TF_DIR}" init
  if [[ "${#TERRAFORM_VAR_ARGS[@]}" -gt 0 ]]; then
    terraform -chdir="${TF_DIR}" apply "${TERRAFORM_VAR_ARGS[@]}"
  else
    terraform -chdir="${TF_DIR}" apply
  fi
fi

if [[ "${SKIP_SECRET_SYNC}" -eq 0 ]]; then
  bash "${SCRIPT_DIR}/sync_env_secrets_to_secret_manager.sh" --env-file "${ENV_FILE}" --project "${PROJECT_ID}"
fi

ensure_local_ssh_key
VM_HOST="$(resolve_host_from_terraform_or_gcloud)"
VM_USER="$(resolve_vm_user)"

if [[ -z "${VM_HOST}" ]]; then
  echo "Could not resolve VM external IP." >&2
  exit 1
fi

if [[ -z "${VM_USER}" ]]; then
  echo "Could not resolve VM Linux user." >&2
  exit 1
fi

if [[ "${RESET_KNOWN_HOST}" -eq 1 ]]; then
  echo "Removing any stale SSH known_hosts entry for ${VM_HOST}..."
  reset_known_host "${VM_HOST}"
fi

if [[ "${SHOW_RESOLVED}" -eq 1 ]]; then
  cat <<EOF
Resolved bootstrap settings:
  PROJECT_ID=${PROJECT_ID}
  VM_NAME=${VM_NAME}
  VM_ZONE=${VM_ZONE}
  VM_HOST=${VM_HOST}
  VM_USER=${VM_USER}
  VM_REPO_DIR=${VM_REPO_DIR}
  VM_ENV_FILE_PATH=${VM_ENV_FILE_PATH}
  TRANSFER_METHOD=${TRANSFER_METHOD}
  BRANCH=${BRANCH}
EOF
fi

if [[ "${TRANSFER_METHOD}" == "git" ]]; then
  if [[ -z "${REPO_URL}" ]]; then
    REPO_URL="$(git -C "${PROJECT_ROOT}" remote get-url origin 2>/dev/null || true)"
  fi
  if [[ -z "${REPO_URL}" ]]; then
    echo "Could not resolve a repo URL for git transfer mode. Pass --repo-url explicitly." >&2
    exit 1
  fi

  bash "${SCRIPT_DIR}/vm_repo_sync.sh" \
    --vm-user "${VM_USER}" \
    --vm-host "${VM_HOST}" \
    --ssh-key-path "${SSH_KEY_PATH}" \
    --vm-repo-dir "${VM_REPO_DIR}" \
    --repo-url "${REPO_URL}" \
    --branch "${BRANCH}"
else
  bash "${SCRIPT_DIR}/vm_repo_copy.sh" \
    --vm-user "${VM_USER}" \
    --vm-host "${VM_HOST}" \
    --ssh-key-path "${SSH_KEY_PATH}" \
    --vm-repo-dir "${VM_REPO_DIR}" \
    --project-root "${PROJECT_ROOT}"
fi

SSH_OPTS=("-o" "StrictHostKeyChecking=accept-new" "-i" "${SSH_KEY_PATH}")
REMOTE="${VM_USER}@${VM_HOST}"
REMOTE_TFVARS_PATH="${VM_REPO_DIR}/infra/terraform/terraform.tfvars.json"

scp "${SSH_OPTS[@]}" "${TFVARS_PATH}" "${REMOTE}:${REMOTE_TFVARS_PATH}"

if [[ "${SKIP_STACK_START}" -eq 0 ]]; then
  ssh "${SSH_OPTS[@]}" "${REMOTE}" "bash -s" <<REMOTE_SCRIPT
set -euo pipefail

VM_REPO_DIR="${VM_REPO_DIR}"
VM_ENV_FILE_PATH="${VM_ENV_FILE_PATH}"
PROJECT_ID="${PROJECT_ID}"
REMOTE_TFVARS_PATH="${REMOTE_TFVARS_PATH}"
CAPSTONE_STACK_UNIT_PATH="/etc/systemd/system/capstone-stack.service"
STACK_UNIT_WAIT_SECONDS=600
STACK_UNIT_WAIT_INTERVAL_SECONDS=5

wait_for_capstone_stack_unit() {
  local waited=0

  while (( waited < STACK_UNIT_WAIT_SECONDS )); do
    if sudo test -f "\${CAPSTONE_STACK_UNIT_PATH}"; then
      return 0
    fi

    sleep "\${STACK_UNIT_WAIT_INTERVAL_SECONDS}"
    waited=\$((waited + STACK_UNIT_WAIT_INTERVAL_SECONDS))
  done

  echo "Timed out waiting for capstone-stack.service to be created by the VM startup script." >&2
  echo "Startup service diagnostics:" >&2
  sudo systemctl status google-startup-scripts.service --no-pager || true
  sudo journalctl -u google-startup-scripts.service -n 120 --no-pager || true
  exit 1
}

if [[ ! -d "\${VM_REPO_DIR}" ]]; then
  echo "Repo directory does not exist on VM: \${VM_REPO_DIR}" >&2
  exit 1
fi

cd "\${VM_REPO_DIR}"
sudo install -d -m 0750 "\$(dirname "\${VM_ENV_FILE_PATH}")"

./scripts/render_pipeline_env_from_secret_manager.sh \
  --output-file "\${VM_ENV_FILE_PATH}" \
  --tfvars-file "\${REMOTE_TFVARS_PATH}" \
  --env-profile vm \
  --project "\${PROJECT_ID}" \
  --show-keys

wait_for_capstone_stack_unit
sudo systemctl daemon-reload
sudo systemctl restart capstone-stack
sudo docker compose --env-file "\${VM_ENV_FILE_PATH}" -f "\${VM_REPO_DIR}/docker/docker-compose.yml" ps
REMOTE_SCRIPT
fi

echo "VM bootstrap complete."
