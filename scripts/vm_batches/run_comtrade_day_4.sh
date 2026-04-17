#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/common.sh"

prepare_runtime
ops_init_all
run_dataset_batch comtrade comtrade_bootstrap_day_4 --trigger-type manual --bruin-pipeline-name vm_batches.comtrade_day_4 "$@"
