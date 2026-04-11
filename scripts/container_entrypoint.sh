#!/usr/bin/env bash
set -euo pipefail

source /workspace/scripts/google_auth_env.sh
configure_google_auth

exec "$@"
