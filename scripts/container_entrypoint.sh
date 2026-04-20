#!/usr/bin/env bash
set -euo pipefail

export VIRTUAL_ENV="${VIRTUAL_ENV:-/workspace/.venv}"
export UV_PROJECT_ENVIRONMENT="${UV_PROJECT_ENVIRONMENT:-/workspace/.venv}"
export UV_CACHE_DIR="${UV_CACHE_DIR:-/workspace/.uv-cache}"
export PATH="${VIRTUAL_ENV}/bin:/root/.local/bin:${PATH}"

source /workspace/scripts/google_auth_env.sh
configure_google_auth

exec "$@"
