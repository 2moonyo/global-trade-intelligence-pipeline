#!/usr/bin/env bash

DEFAULT_GCP_CREDENTIALS_PATH="${DEFAULT_GCP_CREDENTIALS_PATH:-/var/secrets/google/credentials.json}"

configure_google_auth() {
  local quiet="${1:-}"
  local configured_path="${GOOGLE_APPLICATION_CREDENTIALS:-}"

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

  if [[ "${quiet}" != "--quiet" ]]; then
    echo "No mounted Google credentials file found; relying on ambient ADC or VM metadata auth."
  fi
}

if [[ "${BASH_SOURCE[0]}" == "${0}" ]]; then
  configure_google_auth "${1:-}"
fi
