#!/usr/bin/env bash
set -euo pipefail

# -------- EDIT THESE --------
VM_USER="chromazone"                         # not VM instance name
VM_HOST="104.199.42.249"
VM_REPO_DIR="/var/lib/pipeline/capstone"
VM_ENV_FILE="/etc/capstone/pipeline.env"
REPO_SSH_URL="git@github.com:2moonyo/global-trade-intelligence-pipeline.git"     # required for first clone
SSH_KEY_PATH="/Users/chromazone/.ssh/google_compute_engine"                                   # optional local key; empty uses default ssh agent
# ----------------------------

SSH_OPTS="-o StrictHostKeyChecking=accept-new"
if [ -n "$SSH_KEY_PATH" ]; then
  SSH_OPTS="$SSH_OPTS -i $SSH_KEY_PATH"
fi

echo "Testing SSH connectivity..."
ssh $SSH_OPTS "$VM_USER@$VM_HOST" "echo SSH_OK"

echo "Enter Comtrade API keys (input hidden)."
read -rsp "COMTRADE_API_KEY_DATA: " KEY0; echo
read -rsp "COMTRADE_API_KEY_DATA_A: " KEY1; echo
read -rsp "COMTRADE_API_KEY_DATA_B: " KEY2; echo
read -rsp "COMTRADE_API_KEY_DATA_C (optional): " KEY3; echo

echo "Ensuring VM repo exists and is a git repo..."
ssh $SSH_OPTS "$VM_USER@$VM_HOST" "bash -s" <<REMOTE
set -euo pipefail
VM_REPO_DIR="$VM_REPO_DIR"
REPO_SSH_URL="$REPO_SSH_URL"

cd "\$VM_REPO_DIR"

if [ ! -d ".git" ]; then
  echo "No .git found. Initializing repo in-place (no destructive delete)."
  git init
  git remote remove origin 2>/dev/null || true
  git remote add origin "\$REPO_SSH_URL"
  git fetch origin
  if git ls-remote --exit-code --heads origin cloud_migration >/dev/null 2>&1; then
    git checkout -B cloud_migration origin/cloud_migration
  else
    git checkout -B main origin/main
  fi
else
  git remote set-url origin "\$REPO_SSH_URL"
  git fetch --all
  git pull --ff-only
fi
REMOTE

echo "Ensuring env file exists with secure perms..."
ssh $SSH_OPTS "$VM_USER@$VM_HOST" "sudo touch '$VM_ENV_FILE' && sudo chmod 600 '$VM_ENV_FILE'"

echo "Upserting API keys into VM env file..."
ssh $SSH_OPTS "$VM_USER@$VM_HOST" "sudo bash -s '$VM_ENV_FILE' '$KEY0' '$KEY1' '$KEY2' '$KEY3'" <<'REMOTE'
set -euo pipefail
ENV_FILE="$1"
KEY0="${2-}"
KEY1="${3-}"
KEY2="${4-}"
KEY3="${5-}"

upsert_env() {
local name="$1"
local value="$2"
[ -n "$value" ] || return 0
local escaped
escaped="$(printf '%s' "$value" | sed -e 's/[/&]/\&/g')"

if grep -q "^${name}=" "$ENV_FILE"; then
sed -i "s|^${name}=.*|${name}=${escaped}|" "$ENV_FILE"
else
printf '%s=%s\n' "$name" "$value" >> "$ENV_FILE"
fi
}

upsert_env "COMTRADE_API_KEY_DATA" "$KEY0"
upsert_env "COMTRADE_API_KEY_DATA_A" "$KEY1"
upsert_env "COMTRADE_API_KEY_DATA_B" "$KEY2"
upsert_env "COMTRADE_API_KEY_DATA_C" "$KEY3"
chmod 600 "$ENV_FILE"
REMOTE

echo "Recreating pipeline and orchestrator containers..."
ssh $SSH_OPTS "$VM_USER@$VM_HOST" "cd '$VM_REPO_DIR' && sudo docker compose --env-file '$VM_ENV_FILE' -f docker/docker-compose.yml up -d --force-recreate pipeline orchestrator"

echo "Verifying key variable names inside pipeline container..."
ssh $SSH_OPTS "$VM_USER@$VM_HOST" "cd '$VM_REPO_DIR' && sudo docker compose --env-file '$VM_ENV_FILE' -f docker/docker-compose.yml exec -T pipeline env | grep '^COMTRADE_API_KEY_DATA' | cut -d= -f1 | sort"

echo "Done."
