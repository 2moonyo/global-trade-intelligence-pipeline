#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/common.sh"

prepare_runtime
ops_init_all
run_dataset_batch events events_bootstrap_phase_1 --trigger-type manual --bruin-pipeline-name vm_batches.noncomtrade_phase_1_events "$@"
