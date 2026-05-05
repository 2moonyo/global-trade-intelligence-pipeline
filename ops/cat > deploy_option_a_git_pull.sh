cat > deploy_option_a_git_pull.sh <<'SH'
#!/usr/bin/env bash
set -euo pipefail

# -------- EDIT THESE --------
VM_USER="capstone-vm-eu"
VM_HOST="104.199.42.249"
VM_REPO_DIR="/var/lib/pipeline/capstone"
VM_ENV_FILE="/etc/capstone/pipeline.env"
SSH_KEY_PATH=""  
# ----------------------------

SSH_OPTS="-o StrictHostKeyChecking=accept-new"
if [ -n "$SSH_KEY_PATH" ]; then
  SSH_OPTS="$SSH_OPTS -i $SSH_KEY_PATH"
fi

echo "Testing SSH connectivity..."
ssh $SSH_OPTS "$VM_USER@$VM_HOST" "echo 'SSH OK on $(hostname)'"

echo "Enter Comtrade API keys (input hidden)."
read -rsp "COMTRADE_API_KEY_DATA: " KEY0; echo
read -rsp "COMTRADE_API_KEY_DATA_A: " KEY1; echo
read -rsp "COMTRADE_API_KEY_DATA_B: " KEY2; echo
read -rsp "COMTRADE_API_KEY_DATA_C (optional): " KEY3; echo

echo "Ensuring env file exists with secure perms..."
ssh $SSH_OPTS "$VM_USER@$VM_HOST" "sudo touch '$VM_ENV_FILE' && sudo chmod 600 '$VM_ENV_FILE'"

echo "Pulling latest repo changes on VM..."
ssh $SSH_OPTS "$VM_USER@$VM_HOST" "cd '$VM_REPO_DIR' && git fetch --all && git pull --ff-only"

echo "Upserting API keys into VM env file..."
{
  printf '%s\n' "$KEY0"
  printf '%s\n' "$KEY1"
  printf '%s\n' "$KEY2"
  printf '%s\n' "$KEY3"
} | ssh $SSH_OPTS "$VM_USER@$VM_HOST" "sudo bash -s '$VM_ENV_FILE'" <<'REMOTE'
set -euo pipefail
ENV_FILE="$1"

read -r KEY0
read -r KEY1
read -r KEY2
read -r KEY3

upsert_env() {
  local name="$1"
  local value="$2"
  [ -n "$value" ] || return 0
  local escaped
  escaped="$(printf '%s' "$value" | sed -e 's/[\/&]/\\&/g')"

  if grep -q "^${name}=" "$ENV_FILE"; then
    sed -i "s|^${name}=.*|${name}=${escaped}|" "$ENV_FILE"
  else
    printf '%s=%s\n' "$name" "$value" >> "$ENV_FILE"
  fi
}

upsert_env "COMTRADE_API_KEY_DATA"   "$KEY0"
upsert_env "COMTRADE_API_KEY_DATA_A" "$KEY1"
upsert_env "COMTRADE_API_KEY_DATA_B" "$KEY2"
upsert_env "COMTRADE_API_KEY_DATA_C" "$KEY3"

chmod 600 "$ENV_FILE"
REMOTE

echo "Recreating pipeline and orchestrator containers..."
ssh $SSH_OPTS "$VM_USER@$VM_HOST" "cd '$VM_REPO_DIR' && sudo docker compose --env-file '$VM_ENV_FILE' -f docker/docker-compose.yml up -d --force-recreate pipeline orchestrator"

echo "Verifying key variable names inside pipeline container..."
ssh $SSH_OPTS "$VM_USER@$VM_HOST" "cd '$VM_REPO_DIR' && sudo docker compose --env-file '$VM_ENV_FILE' -f [docker-compose.yml](http://_vscodecontentref_/0) exec -T pipeline env | grep '^COMTRADE_API_KEY_DATA' | cut -d= -f1 | sort"

echo "Done."
SH

chmod +x deploy_option_a_git_pull.sh
./deploy_option_a_git_pull.sh