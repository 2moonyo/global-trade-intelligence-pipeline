#!/usr/bin/env bash
set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

cd "${PROJECT_ROOT}"

export DBT_PROFILES_DIR="${PROJECT_ROOT}"
export PYTHONPATH="${PROJECT_ROOT}:${PYTHONPATH:-}"
export UV_CACHE_DIR="${PROJECT_ROOT}/.uv-cache"

source "${PROJECT_ROOT}/scripts/google_auth_env.sh"
configure_google_auth --quiet

mkdir -p "${PROJECT_ROOT}/.uv-cache" "${PROJECT_ROOT}/target" "${PROJECT_ROOT}/dbt_packages"

run_dbt() {
  dbt "$@" --profiles-dir "${PROJECT_ROOT}" --target "${DBT_TARGET:-bigquery_dev}"
}

ensure_ops_bigquery() {
  python warehouse/ops_store.py ensure-bigquery
}

usage() {
  cat <<'EOF'
Usage: scripts/run_dbt.sh <command>

Commands:
  version
  debug
  parse
  build
  docs-generate
  command <dbt arguments...>
EOF
}

COMMAND="${1:-}"

case "${COMMAND}" in
  version)
    dbt --version
    ;;
  debug)
    run_dbt debug
    ;;
  parse)
    run_dbt parse
    ;;
  build)
    ensure_ops_bigquery
    run_dbt build
    ;;
  docs-generate)
    ensure_ops_bigquery
    run_dbt docs generate
    ;;
  command)
    shift
    run_dbt "$@"
    ;;
  "" | help | --help | -h)
    usage
    ;;
  *)
    usage
    exit 1
    ;;
esac
