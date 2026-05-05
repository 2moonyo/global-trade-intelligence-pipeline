#!/usr/bin/env bash
set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

cd "${PROJECT_ROOT}"

export PYTHONPATH="${PROJECT_ROOT}:${PYTHONPATH:-}"
export UV_CACHE_DIR="${PROJECT_ROOT}/.uv-cache"
export VIRTUAL_ENV="${VIRTUAL_ENV:-${PROJECT_ROOT}/.venv}"
export UV_PROJECT_ENVIRONMENT="${UV_PROJECT_ENVIRONMENT:-${PROJECT_ROOT}/.venv}"
export PATH="${VIRTUAL_ENV}/bin:${PATH}"

source "${PROJECT_ROOT}/scripts/google_auth_env.sh"
configure_google_auth --quiet

mkdir -p "${PROJECT_ROOT}/.uv-cache" "${PROJECT_ROOT}/data" "${PROJECT_ROOT}/logs" "${PROJECT_ROOT}/target"

run_python() {
  local py_bin

  if [[ -n "${VIRTUAL_ENV:-}" && -x "${VIRTUAL_ENV}/bin/python" ]]; then
    py_bin="${VIRTUAL_ENV}/bin/python"
  elif [[ -x "${PROJECT_ROOT}/.venv/bin/python" ]]; then
    py_bin="${PROJECT_ROOT}/.venv/bin/python"
  else
    py_bin="$(command -v python)"
  fi

  "${py_bin}" "$@"
}

imports_check() {
  run_python - <<'PY'
import geopandas
import pandas
import pyarrow
import pyproj
import searoute
import shapely
from google.cloud import bigquery
from google.cloud import storage

print("Pipeline imports are available.")
PY
}

postgres_check() {
  run_python - <<'PY'
import os
import socket

host = os.environ.get("POSTGRES_HOST", "postgres")
port = int(os.environ.get("POSTGRES_PORT", "5432"))

with socket.create_connection((host, port), timeout=5):
    pass

print(f"Connected to {host}:{port}.")
PY
}

ops_init_postgres() {
  run_python warehouse/ops_store.py ensure-postgres
}

ops_init_bigquery() {
  run_python warehouse/ops_store.py ensure-bigquery
}

ops_init_all() {
  run_python warehouse/ops_store.py ensure-all
}

dataset_batch() {
  run_python warehouse/run_dataset_batch.py "$@"
}

batch_queue() {
  run_python warehouse/run_batch_queue.py "$@"
}

bootstrap_non_comtrade() {
  batch_queue bootstrap_phase_1
  batch_queue bootstrap_phase_2
}

country_trade_and_energy() {
  batch_queue country_trade_and_energy
}

portwatch_extract() {
  run_python ingest/portwatch/portwatch_extract.py
}

portwatch_silver() {
  run_python ingest/portwatch/portwatch_silver.py
}

portwatch_cloud_dry_run() {
  run_python warehouse/publish_portwatch_to_gcs.py --skip-bronze --include-auxiliary --dry-run
  run_python warehouse/load_portwatch_to_bigquery.py --source local --dry-run
}

portwatch_cloud() {
  run_python warehouse/publish_portwatch_to_gcs.py --skip-bronze --include-auxiliary
  run_python warehouse/load_portwatch_to_bigquery.py
}

portwatch_refresh_cloud() {
  portwatch_silver
  portwatch_cloud
}

comtrade_silver() {
  run_python ingest/comtrade/comtrade_silver.py
}

comtrade_routing() {
  run_python -m ingest.comtrade.routing
}

comtrade_gcs_with_bronze() {
  run_python warehouse/publish_comtrade_to_gcs.py
}

comtrade_cloud_dry_run() {
  run_python warehouse/publish_comtrade_to_gcs.py --skip-bronze --dry-run
  run_python warehouse/load_comtrade_to_bigquery.py --source local --dry-run
}

comtrade_cloud() {
  run_python warehouse/publish_comtrade_to_gcs.py --skip-bronze
  run_python warehouse/load_comtrade_to_bigquery.py
}

comtrade_refresh_cloud() {
  comtrade_silver
  comtrade_routing
  comtrade_cloud
}

brent_extract() {
  run_python ingest/fred/brent_crude.py
}

brent_silver() {
  run_python ingest/fred/brent_silver.py
}

brent_cloud_dry_run() {
  run_python warehouse/publish_brent_to_gcs.py --skip-bronze --dry-run
  run_python warehouse/load_brent_to_bigquery.py --source local --dry-run
}

brent_cloud() {
  run_python warehouse/publish_brent_to_gcs.py --skip-bronze
  run_python warehouse/load_brent_to_bigquery.py
}

brent_refresh_cloud() {
  brent_silver
  brent_cloud
}

fx_extract() {
  run_python ingest/fred/fx_rates.py
}

fx_silver() {
  run_python ingest/fred/fx_silver.py
}

fx_cloud_dry_run() {
  run_python warehouse/publish_fx_to_gcs.py --skip-bronze --dry-run
  run_python warehouse/load_fx_to_bigquery.py --source local --dry-run
}

fx_cloud() {
  run_python warehouse/publish_fx_to_gcs.py --skip-bronze
  run_python warehouse/load_fx_to_bigquery.py
}

fx_refresh_cloud() {
  fx_silver
  fx_cloud
}

events_silver() {
  run_python ingest/events/events_silver.py
}

events_cloud_dry_run() {
  run_python warehouse/publish_events_to_gcs.py --dry-run
  run_python warehouse/load_events_to_bigquery.py --source local --dry-run
}

events_cloud() {
  run_python warehouse/publish_events_to_gcs.py
  run_python warehouse/load_events_to_bigquery.py
}

events_refresh_cloud() {
  events_silver
  events_cloud
}

worldbank_energy_extract() {
  run_python ingest/world_bank/worldbank_energy.py extract
}

worldbank_energy_silver() {
  run_python ingest/world_bank/worldbank_energy_silver.py
}

worldbank_energy_cloud_dry_run() {
  run_python warehouse/publish_worldbank_energy_to_gcs.py --skip-bronze --dry-run
  run_python warehouse/load_worldbank_energy_to_bigquery.py --source local --dry-run
}

worldbank_energy_cloud() {
  run_python warehouse/publish_worldbank_energy_to_gcs.py --skip-bronze
  run_python warehouse/load_worldbank_energy_to_bigquery.py
}

worldbank_energy_refresh_cloud() {
  worldbank_energy_silver
  worldbank_energy_cloud
}

all_cloud_dry_run() {
  portwatch_cloud_dry_run
  comtrade_cloud_dry_run
  brent_cloud_dry_run
  fx_cloud_dry_run
  events_cloud_dry_run
  worldbank_energy_cloud_dry_run
}

all_refresh_cloud() {
  portwatch_refresh_cloud
  brent_refresh_cloud
  comtrade_refresh_cloud
  fx_refresh_cloud
  events_refresh_cloud
  worldbank_energy_refresh_cloud
}

usage() {
  cat <<'EOF'
Usage: scripts/run_pipeline.sh <command>

Commands:
  imports-check
  postgres-check
  ops-init-postgres
  ops-init-bigquery
  ops-init-all
  bootstrap-non-comtrade
  country-trade-and-energy
  portwatch-extract
  portwatch-silver
  portwatch-cloud
  portwatch-cloud-dry-run
  portwatch-refresh-cloud
  comtrade-silver
  comtrade-routing
  comtrade-gcs-with-bronze
  comtrade-cloud
  comtrade-cloud-dry-run
  comtrade-refresh-cloud
  brent-extract
  brent-silver
  brent-cloud
  brent-cloud-dry-run
  brent-refresh-cloud
  fx-extract
  fx-silver
  fx-cloud
  fx-cloud-dry-run
  fx-refresh-cloud
  events-silver
  events-cloud
  events-cloud-dry-run
  events-refresh-cloud
  worldbank-energy-extract
  worldbank-energy-silver
  worldbank-energy-cloud
  worldbank-energy-cloud-dry-run
  worldbank-energy-refresh-cloud
  all-cloud-dry-run
  all-refresh-cloud
  dataset-batch <dataset> <batch_id> [--start-at-task <task>|--start-at-step-order <n>] [runner arguments...]
  batch-queue <schedule_lane> [runner arguments...]
  python <module-or-script arguments...>
EOF
}

COMMAND="${1:-}"

case "${COMMAND}" in
  imports-check) imports_check ;;
  postgres-check) postgres_check ;;
  ops-init-postgres) ops_init_postgres ;;
  ops-init-bigquery) ops_init_bigquery ;;
  ops-init-all) ops_init_all ;;
  bootstrap-non-comtrade) bootstrap_non_comtrade ;;
  country-trade-and-energy) country_trade_and_energy ;;
  portwatch-extract) portwatch_extract ;;
  portwatch-silver) portwatch_silver ;;
  portwatch-cloud) portwatch_cloud ;;
  portwatch-cloud-dry-run) portwatch_cloud_dry_run ;;
  portwatch-refresh-cloud) portwatch_refresh_cloud ;;
  comtrade-silver) comtrade_silver ;;
  comtrade-routing) comtrade_routing ;;
  comtrade-gcs-with-bronze) comtrade_gcs_with_bronze ;;
  comtrade-cloud) comtrade_cloud ;;
  comtrade-cloud-dry-run) comtrade_cloud_dry_run ;;
  comtrade-refresh-cloud) comtrade_refresh_cloud ;;
  brent-extract) brent_extract ;;
  brent-silver) brent_silver ;;
  brent-cloud) brent_cloud ;;
  brent-cloud-dry-run) brent_cloud_dry_run ;;
  brent-refresh-cloud) brent_refresh_cloud ;;
  fx-extract) fx_extract ;;
  fx-silver) fx_silver ;;
  fx-cloud) fx_cloud ;;
  fx-cloud-dry-run) fx_cloud_dry_run ;;
  fx-refresh-cloud) fx_refresh_cloud ;;
  events-silver) events_silver ;;
  events-cloud) events_cloud ;;
  events-cloud-dry-run) events_cloud_dry_run ;;
  events-refresh-cloud) events_refresh_cloud ;;
  worldbank-energy-extract) worldbank_energy_extract ;;
  worldbank-energy-silver) worldbank_energy_silver ;;
  worldbank-energy-cloud) worldbank_energy_cloud ;;
  worldbank-energy-cloud-dry-run) worldbank_energy_cloud_dry_run ;;
  worldbank-energy-refresh-cloud) worldbank_energy_refresh_cloud ;;
  all-cloud-dry-run) all_cloud_dry_run ;;
  all-refresh-cloud) all_refresh_cloud ;;
  dataset-batch)
    shift
    dataset_batch "$@"
    ;;
  batch-queue)
    shift
    batch_queue "$@"
    ;;
  python)
    shift
    run_python "$@"
    ;;
  "" | help | --help | -h)
    usage
    ;;
  *)
    usage
    exit 1
    ;;
esac
