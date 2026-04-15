#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Sync the project repo on a remote VM over SSH.

Usage:
  scripts/vm_repo_sync.sh --vm-user USER --vm-host HOST [options]

Required:
  --vm-user USER                  Linux user on VM
  --vm-host HOST                  VM external IP or DNS

Optional:
  --ssh-key-path PATH             Local private SSH key path (default: use ssh agent/default keys)
  --vm-repo-dir PATH              Repo directory on VM (default: /var/lib/pipeline/capstone)
  --repo-url URL                  Git remote URL used when repo is first initialized
  --branch NAME                   Preferred branch to checkout/pull (default: cloud_migration)
  --commit SHA                    Optional commit SHA to checkout (detached HEAD)
  --help                          Show this help

Notes:
  - If the VM repo directory has no .git, this script initializes git in place.
  - If --commit is provided, the script checks out that commit and does not pull branch head.
  - For private repos, VM must already have GitHub access (deploy key or equivalent).
EOF
}

VM_USER=""
VM_HOST=""
SSH_KEY_PATH=""
VM_REPO_DIR="/var/lib/pipeline/capstone"
REPO_URL=""
BRANCH="cloud_migration"
COMMIT_SHA=""

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
    --repo-url)
      REPO_URL="$2"
      shift 2
      ;;
    --branch)
      BRANCH="$2"
      shift 2
      ;;
    --commit)
      COMMIT_SHA="$2"
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

if [[ -z "$VM_USER" || -z "$VM_HOST" ]]; then
  echo "Error: --vm-user and --vm-host are required." >&2
  usage
  exit 2
fi

SSH_OPTS=("-o" "StrictHostKeyChecking=accept-new")
if [[ -n "$SSH_KEY_PATH" ]]; then
  if [[ ! -f "$SSH_KEY_PATH" ]]; then
    echo "Error: SSH key path does not exist: $SSH_KEY_PATH" >&2
    exit 2
  fi
  if [[ "$SSH_KEY_PATH" == *.pub ]]; then
    echo "Error: --ssh-key-path must be a private key, not a .pub file." >&2
    exit 2
  fi
  SSH_OPTS+=("-i" "$SSH_KEY_PATH")
fi

REMOTE="${VM_USER}@${VM_HOST}"

echo "Testing SSH connectivity..."
ssh "${SSH_OPTS[@]}" "$REMOTE" "echo SSH_OK"

echo "Syncing repository on VM..."
ssh "${SSH_OPTS[@]}" "$REMOTE" "bash -s" <<REMOTE_SCRIPT
set -euo pipefail

VM_REPO_DIR="$VM_REPO_DIR"
REPO_URL="$REPO_URL"
BRANCH="$BRANCH"
COMMIT_SHA="$COMMIT_SHA"

mkdir -p "\$VM_REPO_DIR"
cd "\$VM_REPO_DIR"

if [[ ! -d .git ]]; then
  if [[ -z "\$REPO_URL" ]]; then
    echo "Error: No .git found in \$VM_REPO_DIR and --repo-url was not provided." >&2
    exit 2
  fi

  echo "No .git found. Initializing repo in place..."
  git init
  git remote remove origin 2>/dev/null || true
  git remote add origin "\$REPO_URL"
  git fetch origin

  if git ls-remote --exit-code --heads origin "\$BRANCH" >/dev/null 2>&1; then
    git checkout -B "\$BRANCH" "origin/\$BRANCH"
  elif git ls-remote --exit-code --heads origin main >/dev/null 2>&1; then
    git checkout -B main origin/main
  elif git ls-remote --exit-code --heads origin master >/dev/null 2>&1; then
    git checkout -B master origin/master
  else
    echo "Error: Could not find branch '\$BRANCH', 'main', or 'master' on origin." >&2
    exit 2
  fi
else
  if [[ -n "\$REPO_URL" ]]; then
    git remote set-url origin "\$REPO_URL"
  fi

  git fetch --all --prune

  if git ls-remote --exit-code --heads origin "\$BRANCH" >/dev/null 2>&1; then
    git checkout "\$BRANCH" 2>/dev/null || git checkout -B "\$BRANCH" "origin/\$BRANCH"
    git pull --ff-only origin "\$BRANCH"
  else
    CURRENT_BRANCH="\$(git branch --show-current || true)"
    if [[ -n "\$CURRENT_BRANCH" ]]; then
      git checkout "\$CURRENT_BRANCH"
      git pull --ff-only origin "\$CURRENT_BRANCH"
    else
      echo "Error: Preferred branch '\$BRANCH' not found and current branch is unknown." >&2
      exit 2
    fi
  fi
fi

if [[ -n "\$COMMIT_SHA" ]]; then
  if ! git rev-parse --verify "\$COMMIT_SHA^{commit}" >/dev/null 2>&1; then
    git fetch origin "\$COMMIT_SHA" || true
  fi

  if ! git rev-parse --verify "\$COMMIT_SHA^{commit}" >/dev/null 2>&1; then
    echo "Error: Commit not found: \$COMMIT_SHA" >&2
    exit 2
  fi

  git checkout --detach "\$COMMIT_SHA"
  echo "Repo synced and pinned to commit: \$(git rev-parse --short HEAD)"
else
  echo "Repo sync complete at commit: \$(git rev-parse --short HEAD)"
fi
REMOTE_SCRIPT

echo "Done."
