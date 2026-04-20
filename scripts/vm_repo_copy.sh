#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

usage() {
  cat <<'EOF'
Copy the current local repo contents onto a remote VM over SSH.

Usage:
  scripts/vm_repo_copy.sh --vm-user USER --vm-host HOST [options]

Required:
  --vm-user USER                  Linux user on VM
  --vm-host HOST                  VM external IP or DNS

Optional:
  --ssh-key-path PATH             Local private SSH key path (default: use ssh agent/default keys)
  --vm-repo-dir PATH              Repo directory on VM (default: /var/lib/pipeline/capstone)
  --project-root PATH             Local repo root to copy (default: current repo root)
  --help                          Show this help

Notes:
  - This path does not require GitHub access from the VM.
  - It intentionally excludes local runtime/cache directories and local secret files.
  - It overlays files into the remote repo dir and does not delete existing runtime data.
EOF
}

VM_USER=""
VM_HOST=""
SSH_KEY_PATH=""
VM_REPO_DIR="/var/lib/pipeline/capstone"
LOCAL_PROJECT_ROOT="${PROJECT_ROOT}"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --vm-user)
      VM_USER="$2"
      shift 2
      ;;
    --vm-host)
      VM_HOST="$2"
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
    --project-root)
      LOCAL_PROJECT_ROOT="$2"
      shift 2
      ;;
    --help|-h)
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

if [[ -z "${VM_USER}" || -z "${VM_HOST}" ]]; then
  echo "Error: --vm-user and --vm-host are required." >&2
  usage
  exit 2
fi

if [[ ! -d "${LOCAL_PROJECT_ROOT}" ]]; then
  echo "Error: project root does not exist: ${LOCAL_PROJECT_ROOT}" >&2
  exit 2
fi

SSH_OPTS=("-o" "StrictHostKeyChecking=accept-new")
if [[ -n "${SSH_KEY_PATH}" ]]; then
  if [[ ! -f "${SSH_KEY_PATH}" ]]; then
    echo "Error: SSH key path does not exist: ${SSH_KEY_PATH}" >&2
    exit 2
  fi
  if [[ "${SSH_KEY_PATH}" == *.pub ]]; then
    echo "Error: --ssh-key-path must be a private key, not a .pub file." >&2
    exit 2
  fi
  SSH_OPTS+=("-i" "${SSH_KEY_PATH}")
fi

REMOTE="${VM_USER}@${VM_HOST}"

echo "Testing SSH connectivity..."
ssh "${SSH_OPTS[@]}" "${REMOTE}" "echo SSH_OK"

echo "Preparing repo directory ownership on VM..."
ssh "${SSH_OPTS[@]}" "${REMOTE}" "sudo install -d -m 0755 '${VM_REPO_DIR}' && sudo chown '${VM_USER}':'${VM_USER}' '${VM_REPO_DIR}'"

echo "Copying local repo contents to ${REMOTE}:${VM_REPO_DIR} ..."
(
  cd "${LOCAL_PROJECT_ROOT}"
  COPYFILE_DISABLE=1 COPY_EXTENDED_ATTRIBUTES_DISABLE=1 tar -cf - \
    --exclude='.DS_Store' \
    --exclude='._*' \
    --exclude='.idea' \
    --exclude='.vscode' \
    --exclude='.venv' \
    --exclude='.uv-cache' \
    --exclude='.secrets' \
    --exclude='__pycache__' \
    --exclude='*.pyc' \
    --exclude='*.pyo' \
    --exclude='*.pyd' \
    --exclude='*.log' \
    --exclude='*.zip' \
    --exclude='.env' \
    --exclude='.env.bak' \
    --exclude='data' \
    --exclude='logs' \
    --exclude='target' \
    --exclude='dbt_packages' \
    --exclude='runtime' \
    --exclude='Documentation' \
    --exclude='notebooks' \
    --exclude='infra/terraform/.terraform' \
    --exclude='infra/terraform/*.tfstate' \
    --exclude='*.MD' \
    --exclude='*.md' \
    --exclude='check_db.py' \
    --exclude='deploy_option_a_git_pull.sh' \
    --exclude='infra/terraform/*.tfstate.*' \
    --include='README.md' \
    --include='README.MD' \
    .
) | ssh "${SSH_OPTS[@]}" "${REMOTE}" "tar -xf - -C '${VM_REPO_DIR}'"

EVENTS_SEED_PATH="data/seed/events/events_seed.csv"
if [[ -f "${LOCAL_PROJECT_ROOT}/${EVENTS_SEED_PATH}" ]]; then
  echo "Copying required seed file ${EVENTS_SEED_PATH} ..."
  (
    cd "${LOCAL_PROJECT_ROOT}"
    COPYFILE_DISABLE=1 COPY_EXTENDED_ATTRIBUTES_DISABLE=1 tar -cf - "${EVENTS_SEED_PATH}"
  ) | ssh "${SSH_OPTS[@]}" "${REMOTE}" "tar -xf - -C '${VM_REPO_DIR}'"
else
  echo "Warning: required seed file not found locally: ${EVENTS_SEED_PATH}" >&2
fi

echo "Repo copy complete."
