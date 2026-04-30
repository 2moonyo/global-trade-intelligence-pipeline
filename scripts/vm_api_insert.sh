#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Insert or update API/env keys in the VM runtime env file.

Usage:
  scripts/vm_api_insert.sh --vm-user USER --vm-host HOST [options]

Required:
  --vm-user USER                  Linux user on VM
  --vm-host HOST                  VM external IP or DNS

Optional:
  --ssh-key-path PATH             Local private SSH key path
  --vm-env-file PATH              VM env file path (default: /etc/capstone/pipeline.env)
  --set NAME=VALUE                Upsert one env var; repeat for multiple vars
  --interactive-comtrade          Prompt for COMTRADE_API_KEY_DATA and optional _A/_B/_C
  --interactive-fred              Prompt for FRED_API_KEY
  --show-keys                     Print matching key names after update (no values)
  --help                          Show this help

Examples:
  scripts/vm_api_insert.sh --vm-user chromazone --vm-host 104.199.42.249 \
    --ssh-key-path "$HOME/.ssh/google_compute_engine" \
    --interactive-comtrade --interactive-fred --show-keys

  scripts/vm_api_insert.sh --vm-user chromazone --vm-host 104.199.42.249 \
    --set COMTRADE_API_KEY_DATA=xxx --set COMTRADE_API_KEY_DATA_A=yyy --show-keys
EOF
}

VM_USER=""
VM_HOST=""
SSH_KEY_PATH=""
VM_ENV_FILE="/etc/capstone/pipeline.env"
INTERACTIVE_COMTRADE=0
INTERACTIVE_FRED=0
SHOW_KEYS=0

declare -a SETS

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
    --vm-env-file)
      VM_ENV_FILE="$2"
      shift 2
      ;;
    --set)
      SETS+=("$2")
      shift 2
      ;;
    --interactive-comtrade)
      INTERACTIVE_COMTRADE=1
      shift
      ;;
    --interactive-fred)
      INTERACTIVE_FRED=1
      shift
      ;;
    --show-keys)
      SHOW_KEYS=1
      shift
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

if [[ "$INTERACTIVE_COMTRADE" -eq 1 ]]; then
  read -rsp "COMTRADE_API_KEY_DATA: " KEY0; echo
  read -rsp "COMTRADE_API_KEY_DATA_A (optional): " KEY1; echo
  read -rsp "COMTRADE_API_KEY_DATA_B (optional): " KEY2; echo
  read -rsp "COMTRADE_API_KEY_DATA_C (optional): " KEY3; echo
  [[ -n "$KEY0" ]] && SETS+=("COMTRADE_API_KEY_DATA=$KEY0")
  [[ -n "$KEY1" ]] && SETS+=("COMTRADE_API_KEY_DATA_A=$KEY1")
  [[ -n "$KEY2" ]] && SETS+=("COMTRADE_API_KEY_DATA_B=$KEY2")
  [[ -n "$KEY3" ]] && SETS+=("COMTRADE_API_KEY_DATA_C=$KEY3")
fi

if [[ "$INTERACTIVE_FRED" -eq 1 ]]; then
  read -rsp "FRED_API_KEY: " FRED_KEY; echo
  [[ -n "$FRED_KEY" ]] && SETS+=("FRED_API_KEY=$FRED_KEY")
fi

if [[ ${#SETS[@]} -eq 0 ]]; then
  echo "Error: no keys to write. Use --set and/or interactive flags." >&2
  exit 2
fi

REMOTE="${VM_USER}@${VM_HOST}"

echo "Testing SSH connectivity..."
ssh "${SSH_OPTS[@]}" "$REMOTE" "echo SSH_OK"

for pair in "${SETS[@]}"; do
  if [[ "$pair" != *=* ]]; then
    echo "Error: invalid --set format '$pair' (expected NAME=VALUE)." >&2
    exit 2
  fi
done

echo "Writing keys into VM env file: $VM_ENV_FILE"
ssh "${SSH_OPTS[@]}" "$REMOTE" "sudo bash -s '$VM_ENV_FILE' '${SETS[@]}'" <<'REMOTE_SCRIPT'
set -euo pipefail

ENV_FILE="$1"
shift

sudo install -d -m 0750 "$(dirname "$ENV_FILE")"
sudo touch "$ENV_FILE"
sudo chmod 600 "$ENV_FILE"

for pair in "$@"; do
  [[ -n "$pair" ]] || continue
  if [[ "$pair" != *=* ]]; then
    echo "Skipping invalid pair: $pair" >&2
    continue
  fi

  name="${pair%%=*}"
  value="${pair#*=}"

  escaped="$(printf '%s' "$value" | sed -e 's/[\/&]/\\&/g')"

  if sudo grep -q "^${name}=" "$ENV_FILE"; then
    sudo sed -i "s|^${name}=.*|${name}=${escaped}|" "$ENV_FILE"
  else
    printf '%s=%s\n' "$name" "$value" | sudo tee -a "$ENV_FILE" >/dev/null
  fi
done

sudo chmod 600 "$ENV_FILE"
REMOTE_SCRIPT

if [[ "$SHOW_KEYS" -eq 1 ]]; then
  echo "Key names present in VM env file:"
  ssh "${SSH_OPTS[@]}" "$REMOTE" "sudo grep -E '^(COMTRADE_API_KEY_DATA|FRED_API_KEY)' '$VM_ENV_FILE' | cut -d= -f1 | sort -u"
fi

echo "Done."
