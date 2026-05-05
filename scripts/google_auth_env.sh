#!/usr/bin/env bash

DEFAULT_GCP_CREDENTIALS_PATH="${DEFAULT_GCP_CREDENTIALS_PATH:-/var/secrets/google/credentials.json}"
DEFAULT_GOOGLE_AUTH_MODE="${DEFAULT_GOOGLE_AUTH_MODE:-auto}"

_normalized_google_auth_mode() {
  local mode="${GOOGLE_AUTH_MODE:-${DEFAULT_GOOGLE_AUTH_MODE}}"
  mode="${mode,,}"
  case "${mode}" in
    auto|"")
      printf 'auto\n'
      ;;
    local|local_adc|adc)
      printf 'local_adc\n'
      ;;
    vm|metadata|vm_metadata)
      printf 'vm_metadata\n'
      ;;
    *)
      printf '%s\n' "${mode}"
      ;;
  esac
}

configure_google_auth() {
  local quiet="${1:-}"
  local auth_mode
  local configured_path="${GOOGLE_APPLICATION_CREDENTIALS:-}"
  auth_mode="$(_normalized_google_auth_mode)"

  case "${auth_mode}" in
    vm_metadata)
      unset GOOGLE_APPLICATION_CREDENTIALS
      if [[ "${quiet}" != "--quiet" ]]; then
        echo "GOOGLE_AUTH_MODE=vm_metadata; ignoring key files and relying on VM metadata auth."
      fi
      return 0
      ;;
    auto|local_adc)
      ;;
    *)
      echo "Unsupported GOOGLE_AUTH_MODE='${auth_mode}'. Use one of: auto, local_adc, vm_metadata." >&2
      return 1
      ;;
  esac

  if [[ -n "${configured_path}" ]]; then
    if [[ -f "${configured_path}" ]]; then
      if [[ "${quiet}" != "--quiet" ]]; then
        echo "Using Google credentials from ${configured_path}."
      fi
      return 0
    fi

    if [[ "${quiet}" != "--quiet" ]]; then
      echo "GOOGLE_APPLICATION_CREDENTIALS points to a missing file; unsetting it to allow ADC fallback."
    fi
    unset GOOGLE_APPLICATION_CREDENTIALS
  fi

  if [[ -f "${DEFAULT_GCP_CREDENTIALS_PATH}" ]]; then
    export GOOGLE_APPLICATION_CREDENTIALS="${DEFAULT_GCP_CREDENTIALS_PATH}"
    if [[ "${quiet}" != "--quiet" ]]; then
      echo "Using mounted Google credentials file at ${GOOGLE_APPLICATION_CREDENTIALS}."
    fi
    return 0
  fi

  if [[ "${auth_mode}" == "local_adc" ]]; then
    echo "GOOGLE_AUTH_MODE=local_adc requires a mounted credentials file at ${DEFAULT_GCP_CREDENTIALS_PATH} or a valid GOOGLE_APPLICATION_CREDENTIALS path." >&2
    return 1
  fi

  if [[ "${quiet}" != "--quiet" ]]; then
    echo "No mounted Google credentials file found; relying on ambient ADC or VM metadata auth."
  fi
}

if [[ "${BASH_SOURCE[0]}" == "${0}" ]]; then
  configure_google_auth "${1:-}"
fi
