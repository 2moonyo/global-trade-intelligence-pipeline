#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Usage: scripts/render_pipeline_env_from_secret_manager.sh [options]

Render or refresh a runtime env file by preserving non-secret settings from a
base env file and replacing approved keys with the latest Secret Manager values.

Defaults:
  --output-file /etc/capstone/pipeline.env
  --base-env-file <output-file if it exists, otherwise ops/vm/pipeline.env.example>
  --project from GCP_PROJECT_ID in the base env file, otherwise gcloud config

Options:
  --output-file PATH      Runtime env file to write
  --base-env-file PATH    Base env file to preserve non-secret settings from
  --project PROJECT_ID    GCP project id for Secret Manager lookups
  --strict-secrets        Fail if any approved secret is missing
  --show-keys             Print the approved key names present after render
  -h, --help              Show this help
EOF
}

OUTPUT_FILE="/etc/capstone/pipeline.env"
BASE_ENV_FILE=""
PROJECT_ID=""
STRICT_SECRETS=0
SHOW_KEYS=0

while [[ $# -gt 0 ]]; do
  case "$1" in
    --output-file)
      OUTPUT_FILE="$2"
      shift 2
      ;;
    --base-env-file)
      BASE_ENV_FILE="$2"
      shift 2
      ;;
    --project)
      PROJECT_ID="$2"
      shift 2
      ;;
    --strict-secrets)
      STRICT_SECRETS=1
      shift
      ;;
    --show-keys)
      SHOW_KEYS=1
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

if [[ -z "${BASE_ENV_FILE}" ]]; then
  if [[ -f "${OUTPUT_FILE}" ]]; then
    BASE_ENV_FILE="${OUTPUT_FILE}"
  else
    BASE_ENV_FILE="ops/vm/pipeline.env.example"
  fi
fi

if [[ ! -f "${BASE_ENV_FILE}" ]]; then
  echo "Base env file not found: ${BASE_ENV_FILE}" >&2
  exit 1
fi

if ! command -v gcloud >/dev/null 2>&1; then
  echo "gcloud CLI is required but not found in PATH." >&2
  exit 1
fi

secret_id_for_key() {
  case "$1" in
    FRED_API_KEY) echo "capstone-fred-api-key" ;;
    COMTRADE_API_KEY) echo "capstone-comtrade-api-key" ;;
    COMTRADE_API_KEY_DATA) echo "capstone-comtrade-api-key-data" ;;
    COMTRADE_API_KEY_DATA_A) echo "capstone-comtrade-api-key-data-a" ;;
    COMTRADE_API_KEY_DATA_B) echo "capstone-comtrade-api-key-data-b" ;;
    POSTGRES_USER) echo "capstone-postgres-user" ;;
    POSTGRES_PASSWORD) echo "capstone-postgres-password" ;;
    POSTGRES_DB) echo "capstone-postgres-db" ;;
    POSTGRES_SCHEMA) echo "capstone-postgres-schema" ;;
    *)
      echo "Unsupported managed key: $1" >&2
      return 1
      ;;
  esac
}

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

TMP_BASE="$(mktemp)"
TMP_OUTPUT="$(mktemp)"
TMP_SECRETS="$(mktemp)"
cleanup() {
  rm -f "${TMP_BASE}" "${TMP_OUTPUT}" "${TMP_SECRETS}"
}
trap cleanup EXIT

if [[ -r "${BASE_ENV_FILE}" ]]; then
  cp "${BASE_ENV_FILE}" "${TMP_BASE}"
else
  sudo cat "${BASE_ENV_FILE}" > "${TMP_BASE}"
fi

read_env_value() {
  local env_file="$1"
  local key="$2"
  python3 - "${env_file}" "${key}" <<'PY'
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

if [[ -z "${PROJECT_ID}" ]]; then
  PROJECT_ID="$(read_env_value "${TMP_BASE}" "GCP_PROJECT_ID")"
fi

if [[ "${PROJECT_ID}" == "your-gcp-project-id" ]]; then
  PROJECT_ID=""
fi

if [[ -z "${PROJECT_ID}" ]]; then
  PROJECT_ID="$(gcloud config get-value project 2>/dev/null || true)"
fi

if [[ -z "${PROJECT_ID}" ]]; then
  echo "Could not resolve project id. Pass --project or set GCP_PROJECT_ID in ${BASE_ENV_FILE}." >&2
  exit 1
fi

declare -a SECRET_PAIRS
declare -a MISSING_KEYS

echo "Rendering ${OUTPUT_FILE} from base ${BASE_ENV_FILE} with Secret Manager project ${PROJECT_ID}"
for key in "${KEYS[@]}"; do
  secret_id="$(secret_id_for_key "${key}")"
  secret_value=""
  if secret_value="$(gcloud secrets versions access latest --secret="${secret_id}" --project="${PROJECT_ID}" 2>/dev/null)" && [[ -n "${secret_value}" ]]; then
    if [[ "${secret_value}" == *$'\n'* ]]; then
      echo "Secret ${secret_id} for ${key} contains a newline and cannot be written to an env file safely." >&2
      exit 1
    fi
    SECRET_PAIRS+=("${key}=${secret_value}")
    echo "- will update ${key} from ${secret_id}"
  else
    MISSING_KEYS+=("${key}")
    echo "- missing ${key}; secret ${secret_id} unavailable"
  fi
done

if [[ "${STRICT_SECRETS}" -eq 1 && "${#MISSING_KEYS[@]}" -gt 0 ]]; then
  echo "Missing required secrets: ${MISSING_KEYS[*]}" >&2
  exit 1
fi

printf '' > "${TMP_SECRETS}"
chmod 600 "${TMP_SECRETS}"
for pair in "${SECRET_PAIRS[@]}"; do
  printf '%s\n' "${pair}" >> "${TMP_SECRETS}"
done

python3 - "${TMP_BASE}" "${TMP_OUTPUT}" "${TMP_SECRETS}" "${KEYS[@]}" <<'PY'
import pathlib
import sys

base_path = pathlib.Path(sys.argv[1])
output_path = pathlib.Path(sys.argv[2])
secrets_path = pathlib.Path(sys.argv[3])

ordered_keys = sys.argv[4:]

replacements = {}
for pair in secrets_path.read_text(encoding="utf-8").splitlines():
    if not pair:
        continue
    key, value = pair.split("=", 1)
    replacements[key] = value

lines = base_path.read_text(encoding="utf-8").splitlines()
updated = set()
rendered: list[str] = []
for line in lines:
    stripped = line.strip()
    if not stripped or stripped.startswith("#") or "=" not in line:
        rendered.append(line)
        continue
    left, _right = line.split("=", 1)
    key = left.strip()
    if key in replacements:
        rendered.append(f"{key}={replacements[key]}")
        updated.add(key)
    else:
        rendered.append(line)

for key in ordered_keys:
    if key in replacements and key not in updated:
        rendered.append(f"{key}={replacements[key]}")

output_path.write_text("\n".join(rendered) + "\n", encoding="utf-8")
PY

if [[ -w "$(dirname "${OUTPUT_FILE}")" ]] && [[ ! -e "${OUTPUT_FILE}" || -w "${OUTPUT_FILE}" ]]; then
  install -d -m 0750 "$(dirname "${OUTPUT_FILE}")"
  install -m 600 "${TMP_OUTPUT}" "${OUTPUT_FILE}"
else
  sudo install -d -m 0750 "$(dirname "${OUTPUT_FILE}")"
  sudo install -m 600 "${TMP_OUTPUT}" "${OUTPUT_FILE}"
fi

if [[ "${SHOW_KEYS}" -eq 1 ]]; then
  echo "Approved keys present in ${OUTPUT_FILE}:"
  if [[ -r "${OUTPUT_FILE}" ]]; then
    grep -E '^(FRED_API_KEY|COMTRADE_API_KEY|COMTRADE_API_KEY_DATA|COMTRADE_API_KEY_DATA_A|COMTRADE_API_KEY_DATA_B|POSTGRES_USER|POSTGRES_PASSWORD|POSTGRES_DB|POSTGRES_SCHEMA)=' "${OUTPUT_FILE}" | cut -d= -f1 | sort -u
  else
    sudo grep -E '^(FRED_API_KEY|COMTRADE_API_KEY|COMTRADE_API_KEY_DATA|COMTRADE_API_KEY_DATA_A|COMTRADE_API_KEY_DATA_B|POSTGRES_USER|POSTGRES_PASSWORD|POSTGRES_DB|POSTGRES_SCHEMA)=' "${OUTPUT_FILE}" | cut -d= -f1 | sort -u
  fi
fi

echo "Rendered ${OUTPUT_FILE} successfully."
