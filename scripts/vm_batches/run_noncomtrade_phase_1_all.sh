#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

"${SCRIPT_DIR}/run_noncomtrade_phase_1_portwatch.sh" "$@"
"${SCRIPT_DIR}/run_noncomtrade_phase_1_brent.sh" "$@"
"${SCRIPT_DIR}/run_noncomtrade_phase_1_fx.sh" "$@"
"${SCRIPT_DIR}/run_noncomtrade_phase_1_events.sh" "$@"
