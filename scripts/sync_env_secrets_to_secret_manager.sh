#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Usage: scripts/sync_env_secrets_to_secret_manager.sh [--env-file PATH] [--project PROJECT_ID]

Reads only approved API/POSTGRES keys from an env file and pushes them to
Google Secret Manager as new secret versions.

Defaults:
  --env-file .env
  --project from GCP_PROJECT_ID in env file

Approved keys:
  FRED_API_KEY
  COMTRADE_API_KEY
  COMTRADE_API_KEY_DATA
  COMTRADE_API_KEY_DATA_A
  COMTRADE_API_KEY_DATA_B
  POSTGRES_USER
  POSTGRES_PASSWORD
  POSTGRES_DB
  POSTGRES_SCHEMA
EOF
}

ENV_FILE=".env"
PROJECT_ID=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    --env-file)
      ENV_FILE="$2"
      shift 2
      ;;
    --project)
      PROJECT_ID="$2"
      shift 2
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "Unknown argument: $1" >&2
      usage
      exit 1
      ;;
  esac
done

if [[ ! -f "$ENV_FILE" ]]; then
  echo "Env file not found: $ENV_FILE" >&2
  exit 1
fi

read_env_value() {
  local key="$1"
  python3 - "$ENV_FILE" "$key" <<'PY'
import pathlib
import sys

path = pathlib.Path(sys.argv[1])
key = sys.argv[2]
value = ""

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

if [[ -z "$PROJECT_ID" ]]; then
  PROJECT_ID="$(read_env_value "GCP_PROJECT_ID")"
fi

if [[ -z "$PROJECT_ID" ]]; then
  echo "Missing project id. Pass --project or set GCP_PROJECT_ID in $ENV_FILE." >&2
  exit 1
fi

if ! command -v gcloud >/dev/null 2>&1; then
  echo "gcloud CLI is required but not found in PATH." >&2
  exit 1
fi

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

KEYS=(
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

echo "Syncing selected keys from $ENV_FILE into Secret Manager project $PROJECT_ID"

for key in "${KEYS[@]}"; do
  value="$(read_env_value "$key")"
  if [[ -z "$value" ]]; then
    echo "- skip $key (missing or empty)"
    continue
  fi

  secret_id="${SECRET_BY_KEY[$key]}"

  if ! gcloud secrets describe "$secret_id" --project "$PROJECT_ID" >/dev/null 2>&1; then
    gcloud secrets create "$secret_id" \
      --project "$PROJECT_ID" \
      --replication-policy automatic >/dev/null
    echo "- created secret $secret_id"
  fi

  printf '%s' "$value" | gcloud secrets versions add "$secret_id" \
    --project "$PROJECT_ID" \
    --data-file=- >/dev/null

  echo "- updated $key -> $secret_id"
done

echo "Done. Secret values were not printed."
