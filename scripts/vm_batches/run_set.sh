#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SET_NAME="${1:-}"

usage() {
  cat <<'EOF'
Usage: scripts/vm_batches/run_set.sh <set-name> [run_pipeline dataset-batch extra args...]

Extra args are forwarded to the underlying dataset batch entrypoint.
Examples: --start-at-task silver   or   --start-at-step-order 2

Available set names:
  comtrade-day-1
  comtrade-day-2
  comtrade-day-3
  comtrade-day-4
  comtrade-day-5
  comtrade-day-6
  comtrade-all
  noncomtrade-phase-1-portwatch
  noncomtrade-phase-1-brent
  noncomtrade-phase-1-fx
  noncomtrade-phase-1-events
  noncomtrade-phase-1-all
  noncomtrade-phase-2-portwatch
  noncomtrade-phase-2-brent
  noncomtrade-phase-2-fx
  noncomtrade-phase-2-events
  noncomtrade-phase-2-all
EOF
}

if [[ -z "${SET_NAME}" || "${SET_NAME}" == "-h" || "${SET_NAME}" == "--help" ]]; then
  usage
  exit 0
fi

shift

case "${SET_NAME}" in
  comtrade-day-1) "${SCRIPT_DIR}/run_comtrade_day_1.sh" "$@" ;;
  comtrade-day-2) "${SCRIPT_DIR}/run_comtrade_day_2.sh" "$@" ;;
  comtrade-day-3) "${SCRIPT_DIR}/run_comtrade_day_3.sh" "$@" ;;
  comtrade-day-4) "${SCRIPT_DIR}/run_comtrade_day_4.sh" "$@" ;;
  comtrade-day-5) "${SCRIPT_DIR}/run_comtrade_day_5.sh" "$@" ;;
  comtrade-day-6) "${SCRIPT_DIR}/run_comtrade_day_6.sh" "$@" ;;
  comtrade-all) "${SCRIPT_DIR}/run_comtrade_all_days.sh" "$@" ;;
  noncomtrade-phase-1-portwatch) "${SCRIPT_DIR}/run_noncomtrade_phase_1_portwatch.sh" "$@" ;;
  noncomtrade-phase-1-brent) "${SCRIPT_DIR}/run_noncomtrade_phase_1_brent.sh" "$@" ;;
  noncomtrade-phase-1-fx) "${SCRIPT_DIR}/run_noncomtrade_phase_1_fx.sh" "$@" ;;
  noncomtrade-phase-1-events) "${SCRIPT_DIR}/run_noncomtrade_phase_1_events.sh" "$@" ;;
  noncomtrade-phase-1-all) "${SCRIPT_DIR}/run_noncomtrade_phase_1_all.sh" "$@" ;;
  noncomtrade-phase-2-portwatch) "${SCRIPT_DIR}/run_noncomtrade_phase_2_portwatch.sh" "$@" ;;
  noncomtrade-phase-2-brent) "${SCRIPT_DIR}/run_noncomtrade_phase_2_brent.sh" "$@" ;;
  noncomtrade-phase-2-fx) "${SCRIPT_DIR}/run_noncomtrade_phase_2_fx.sh" "$@" ;;
  noncomtrade-phase-2-events) "${SCRIPT_DIR}/run_noncomtrade_phase_2_events.sh" "$@" ;;
  noncomtrade-phase-2-all) "${SCRIPT_DIR}/run_noncomtrade_phase_2_all.sh" "$@" ;;
  *)
    echo "Unknown set: ${SET_NAME}" >&2
    usage
    exit 1
    ;;
esac
