#!/usr/bin/env bash
set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
TF_DIR="${PROJECT_ROOT}/infra/terraform"
TFVARS_PATH="${TFVARS_PATH:-${TF_DIR}/terraform.tfvars.json}"

usage() {
  cat <<'EOF'
Usage: scripts/vm_runtime_ctl.sh <command> [primary|legacy|recovery|all]

Commands:
  status                     Show configured VM and disk resources. Defaults to all.
  start                      Start the target VM with gcloud. Defaults to primary.
  stop                       Stop the target VM with gcloud. Defaults to primary.
  delete-instance-gcloud     Delete only the target VM instance with gcloud.
  delete-disk-gcloud         Delete only the target persistent data disk with gcloud.
  delete-boot-disk-gcloud    Delete only the target standalone boot disk with gcloud.
  destroy-compute-gcloud     Delete the target VM first, then any managed disks for that target.
  destroy-compute-terraform  Terraform destroy only the compute resources for the target.
EOF
}

require_tfvars() {
  if [[ ! -f "${TFVARS_PATH}" ]]; then
    echo "Missing ${TFVARS_PATH}. Run 'make tfvars-init' first." >&2
    exit 1
  fi
}

read_tfvar() {
  python - "${TFVARS_PATH}" "$1" <<'PY'
import json
import pathlib
import sys

payload = json.loads(pathlib.Path(sys.argv[1]).read_text(encoding="utf-8"))
value = payload.get(sys.argv[2], "")
if value is None:
    value = ""
if isinstance(value, bool):
    print("true" if value else "false")
else:
    print(value)
PY
}

resolve_target() {
  local target="${1}"

  PROJECT_ID="$(read_tfvar project_id)"
  if [[ -z "${PROJECT_ID}" ]]; then
    echo "Expected project_id in ${TFVARS_PATH}." >&2
    exit 1
  fi

  case "${target}" in
    primary)
      TARGET_LABEL="primary"
      VM_ENABLED="$(read_tfvar primary_compute_vm_enabled)"
      VM_NAME="$(read_tfvar primary_vm_name)"
      VM_ZONE="$(read_tfvar primary_zone)"
      DATA_DISK_ENABLED="$(read_tfvar primary_compute_vm_enabled)"
      DATA_DISK_NAME="$(read_tfvar primary_data_disk_name)"
      BOOT_DISK_ENABLED="$(read_tfvar primary_compute_vm_enabled)"
      BOOT_DISK_NAME="$(read_tfvar primary_boot_disk_name)"
      SCHEDULE_ENABLED="$(read_tfvar primary_instance_schedule_enabled)"
      SNAPSHOT_ENABLED="$(read_tfvar primary_snapshot_schedule_enabled)"
      ;;
    legacy)
      TARGET_LABEL="legacy"
      VM_ENABLED="$(read_tfvar legacy_compute_vm_enabled)"
      VM_NAME="$(read_tfvar legacy_vm_name)"
      VM_ZONE="$(read_tfvar legacy_zone)"
      DATA_DISK_ENABLED="$(read_tfvar legacy_compute_vm_enabled)"
      DATA_DISK_NAME="$(read_tfvar legacy_data_disk_name)"
      BOOT_DISK_ENABLED="false"
      BOOT_DISK_NAME=""
      SCHEDULE_ENABLED="$(read_tfvar legacy_instance_schedule_enabled)"
      SNAPSHOT_ENABLED="false"
      ;;
    recovery)
      TARGET_LABEL="recovery"
      VM_ENABLED="$(read_tfvar recovery_vm_enabled)"
      VM_NAME="$(read_tfvar recovery_vm_name)"
      VM_ZONE="$(read_tfvar fallback_zone)"
      DATA_DISK_ENABLED="$(read_tfvar recovery_data_disk_enabled)"
      DATA_DISK_NAME="$(read_tfvar recovery_data_disk_name)"
      BOOT_DISK_ENABLED="$(read_tfvar recovery_boot_disk_enabled)"
      BOOT_DISK_NAME="$(read_tfvar recovery_boot_disk_name)"
      SCHEDULE_ENABLED="false"
      SNAPSHOT_ENABLED="false"
      ;;
    *)
      echo "Unknown target: ${target}" >&2
      usage >&2
      exit 1
      ;;
  esac
}

require_vm_enabled() {
  if [[ "${VM_ENABLED}" != "true" || -z "${VM_NAME}" || -z "${VM_ZONE}" ]]; then
    echo "Target ${TARGET_LABEL} VM is not enabled in ${TFVARS_PATH}." >&2
    exit 1
  fi
}

require_data_disk_enabled() {
  if [[ "${DATA_DISK_ENABLED}" != "true" || -z "${DATA_DISK_NAME}" || -z "${VM_ZONE}" ]]; then
    echo "Target ${TARGET_LABEL} data disk is not enabled in ${TFVARS_PATH}." >&2
    exit 1
  fi
}

require_boot_disk_enabled() {
  if [[ "${BOOT_DISK_ENABLED}" != "true" || -z "${BOOT_DISK_NAME}" || -z "${VM_ZONE}" ]]; then
    echo "Target ${TARGET_LABEL} boot disk is not enabled in ${TFVARS_PATH}." >&2
    exit 1
  fi
}

show_status_for_target() {
  local target="${1}"

  resolve_target "${target}"
  echo "== ${TARGET_LABEL} =="

  if [[ "${VM_ENABLED}" == "true" ]]; then
    gcloud compute instances list --project "${PROJECT_ID}" --filter="name=(${VM_NAME})"
  else
    echo "VM disabled in tfvars."
  fi

  if [[ "${BOOT_DISK_ENABLED}" == "true" ]]; then
    gcloud compute disks list --project "${PROJECT_ID}" --filter="name=(${BOOT_DISK_NAME})"
  fi

  if [[ "${DATA_DISK_ENABLED}" == "true" ]]; then
    gcloud compute disks list --project "${PROJECT_ID}" --filter="name=(${DATA_DISK_NAME})"
  fi
}

status() {
  local target="${1:-all}"

  case "${target}" in
    all)
      show_status_for_target primary
      show_status_for_target legacy
      show_status_for_target recovery
      ;;
    primary|legacy|recovery)
      show_status_for_target "${target}"
      ;;
    *)
      echo "Unknown target: ${target}" >&2
      usage >&2
      exit 1
      ;;
  esac
}

start_vm() {
  local target="${1:-primary}"
  resolve_target "${target}"
  require_vm_enabled
  gcloud compute instances start "${VM_NAME}" --zone "${VM_ZONE}" --project "${PROJECT_ID}"
}

stop_vm() {
  local target="${1:-primary}"
  resolve_target "${target}"
  require_vm_enabled
  gcloud compute instances stop "${VM_NAME}" --zone "${VM_ZONE}" --project "${PROJECT_ID}"
}

delete_instance_gcloud() {
  local target="${1:-primary}"
  resolve_target "${target}"
  require_vm_enabled
  gcloud compute instances delete "${VM_NAME}" --zone "${VM_ZONE}" --project "${PROJECT_ID}"
}

delete_data_disk_gcloud() {
  local target="${1:-primary}"
  resolve_target "${target}"
  require_data_disk_enabled
  gcloud compute disks delete "${DATA_DISK_NAME}" --zone "${VM_ZONE}" --project "${PROJECT_ID}"
}

delete_boot_disk_gcloud() {
  local target="${1:-primary}"
  resolve_target "${target}"
  require_boot_disk_enabled
  gcloud compute disks delete "${BOOT_DISK_NAME}" --zone "${VM_ZONE}" --project "${PROJECT_ID}"
}

destroy_compute_gcloud() {
  local target="${1:-primary}"
  resolve_target "${target}"

  if [[ "${VM_ENABLED}" == "true" ]]; then
    gcloud compute instances delete "${VM_NAME}" --zone "${VM_ZONE}" --project "${PROJECT_ID}"
  fi

  if [[ "${BOOT_DISK_ENABLED}" == "true" ]]; then
    gcloud compute disks delete "${BOOT_DISK_NAME}" --zone "${VM_ZONE}" --project "${PROJECT_ID}"
  fi

  if [[ "${DATA_DISK_ENABLED}" == "true" ]]; then
    gcloud compute disks delete "${DATA_DISK_NAME}" --zone "${VM_ZONE}" --project "${PROJECT_ID}"
  fi
}

destroy_compute_terraform() {
  local target="${1:-primary}"
  local destroy_targets=()

  resolve_target "${target}"

  case "${target}" in
    primary)
      if [[ "${VM_ENABLED}" == "true" ]]; then
        destroy_targets+=(-target=google_compute_instance.primary_vm)
        destroy_targets+=(-target=google_compute_disk.primary_boot_disk)
        destroy_targets+=(-target=google_compute_disk.primary_data_disk)
      fi
      if [[ "${SCHEDULE_ENABLED}" == "true" ]]; then
        destroy_targets+=(-target=google_compute_resource_policy.primary_vm_schedule)
      fi
      if [[ "${SNAPSHOT_ENABLED}" == "true" ]]; then
        destroy_targets+=(-target=google_compute_resource_policy.primary_boot_snapshot_schedule)
        destroy_targets+=(-target=google_compute_resource_policy.primary_data_snapshot_schedule)
        destroy_targets+=(-target=google_compute_disk_resource_policy_attachment.primary_boot_snapshot_schedule)
        destroy_targets+=(-target=google_compute_disk_resource_policy_attachment.primary_data_snapshot_schedule)
      fi
      ;;
    legacy)
      if [[ "${VM_ENABLED}" == "true" ]]; then
        destroy_targets+=(-target=google_compute_instance.free_vm)
        destroy_targets+=(-target=google_compute_disk.data_disk)
      fi
      if [[ "${SCHEDULE_ENABLED}" == "true" ]]; then
        destroy_targets+=(-target=google_compute_resource_policy.vm_schedule)
      fi
      ;;
    recovery)
      if [[ "${VM_ENABLED}" == "true" ]]; then
        destroy_targets+=(-target=google_compute_instance.recovery_vm)
      fi
      if [[ "${BOOT_DISK_ENABLED}" == "true" ]]; then
        destroy_targets+=(-target=google_compute_disk.recovery_boot_disk)
      fi
      if [[ "${DATA_DISK_ENABLED}" == "true" ]]; then
        destroy_targets+=(-target=google_compute_disk.recovery_data_disk)
      fi
      ;;
  esac

  if [[ "${#destroy_targets[@]}" -eq 0 ]]; then
    echo "No Terraform-managed compute resources are enabled for target ${target}." >&2
    exit 1
  fi

  terraform -chdir="${TF_DIR}" init -input=false
  terraform -chdir="${TF_DIR}" destroy "${destroy_targets[@]}"
}

main() {
  local command="${1:-}"
  local target="${2:-}"
  require_tfvars

  case "${command}" in
    status)
      status "${target:-all}"
      ;;
    start)
      start_vm "${target:-primary}"
      ;;
    stop)
      stop_vm "${target:-primary}"
      ;;
    delete-instance-gcloud)
      delete_instance_gcloud "${target:-primary}"
      ;;
    delete-disk-gcloud)
      delete_data_disk_gcloud "${target:-primary}"
      ;;
    delete-boot-disk-gcloud)
      delete_boot_disk_gcloud "${target:-primary}"
      ;;
    destroy-compute-gcloud)
      destroy_compute_gcloud "${target:-primary}"
      ;;
    destroy-compute-terraform)
      destroy_compute_terraform "${target:-primary}"
      ;;
    ""|-h|--help|help)
      usage
      ;;
    *)
      echo "Unknown command: ${command}" >&2
      usage >&2
      exit 1
      ;;
  esac
}

main "$@"
