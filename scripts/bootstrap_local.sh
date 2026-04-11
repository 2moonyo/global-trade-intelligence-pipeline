#!/usr/bin/env bash
set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

cd "${PROJECT_ROOT}"

mkdir -p \
  .secrets/gcp \
  .uv-cache \
  data \
  dbt_packages \
  logs/ops \
  logs/brent \
  logs/comtrade \
  logs/events \
  logs/fx \
  logs/monthly_history \
  logs/portwatch \
  logs/worldbank_energy \
  target

upsert_env_var() {
  local key="$1"
  local value="$2"
  local tmp_file

  if [[ ! -f .env ]]; then
    return 0
  fi

  tmp_file="$(mktemp)"
  awk -v key="${key}" -v value="${value}" '
    BEGIN {
      updated = 0
    }
    index($0, key "=") == 1 {
      print key "=" value
      updated = 1
      next
    }
    {
      print
    }
    END {
      if (!updated) {
        print key "=" value
      }
    }
  ' .env > "${tmp_file}"
  mv "${tmp_file}" .env
}

if [[ ! -f .env ]]; then
  if [[ -f infra/terraform/terraform.tfvars.json ]]; then
    python infra/terraform/render_dotenv.py > .env
    if ! grep -q '^FRED_API_KEY=' .env; then
      printf '\nFRED_API_KEY=\n' >> .env
    fi
    if ! grep -q '^COMTRADE_API_KEY_DATA=' .env; then
      printf 'COMTRADE_API_KEY_DATA=\nCOMTRADE_API_KEY_DATA_A=\nCOMTRADE_API_KEY_DATA_B=\n' >> .env
    fi
    if ! grep -q '^POSTGRES_USER=' .env; then
      printf 'POSTGRES_USER=capstone\nPOSTGRES_PASSWORD=capstone\nPOSTGRES_DB=capstone\n' >> .env
    fi
    echo "Rendered .env from infra/terraform/terraform.tfvars.json."
  else
    cp .env.example .env
    echo "Created .env from .env.example."
  fi
else
  echo ".env already exists."
fi

upsert_env_var "POSTGRES_SCHEMA" "ops"
upsert_env_var "BATCH_PLAN_PATH" "ops/batch_plan.json"
upsert_env_var "ENABLE_BIGQUERY_OPS_MIRROR" "true"
upsert_env_var "OPS_STRICT_BIGQUERY_MIRROR" "false"

HOST_ADC_PATH="${HOME}/.config/gcloud/application_default_credentials.json"

if [[ ! -f .secrets/gcp/credentials.json && -f "${HOST_ADC_PATH}" ]]; then
  cp "${HOST_ADC_PATH}" .secrets/gcp/credentials.json
  echo "Copied local ADC credentials into .secrets/gcp/credentials.json."
elif [[ ! -f .secrets/gcp/credentials.json ]]; then
  echo "No local ADC file found. For local Docker, run 'gcloud auth application-default login' or place a JSON at .secrets/gcp/credentials.json."
  echo "For a GCP VM with an attached service account, you can leave this file absent."
fi

if [[ -f .secrets/gcp/credentials.json ]]; then
  upsert_env_var "GOOGLE_APPLICATION_CREDENTIALS" "/var/secrets/google/credentials.json"
  echo "Configured .env to use the mounted Google credentials path for local Docker runs."
fi

echo "Local container bootstrap complete."
