#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

"${SCRIPT_DIR}/run_comtrade_day_1.sh" "$@"
"${SCRIPT_DIR}/run_comtrade_day_2.sh" "$@"
"${SCRIPT_DIR}/run_comtrade_day_3.sh" "$@"
"${SCRIPT_DIR}/run_comtrade_day_4.sh" "$@"
"${SCRIPT_DIR}/run_comtrade_day_5.sh" "$@"
"${SCRIPT_DIR}/run_comtrade_day_6.sh" "$@"
