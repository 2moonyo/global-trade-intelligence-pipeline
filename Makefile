SHELL := /bin/bash

PROJECT_ROOT := $(CURDIR)
TF_DIR := infra/terraform
TFVARS := $(TF_DIR)/terraform.tfvars.json
TFVARS_EXAMPLE := $(TF_DIR)/terraform.tfvars.json.example

.DEFAULT_GOAL := help

.PHONY: help tfvars-init check-tfvars deps-sync gcp-auth infra-init infra-plan infra-apply infra-destroy env-file env-print cloud-bootstrap portwatch-silver portwatch-cloud-dry-run portwatch-cloud portwatch-refresh-cloud dbt-bigquery-debug dbt-bigquery-build

help:
	@printf "%s\n" \
		"make tfvars-init             Copy the Terraform vars example if needed." \
		"make cloud-bootstrap         ADC auth if needed, terraform init/apply, and render .env." \
		"make infra-destroy           Destroy Terraform-managed cloud resources after confirmation." \
		"make portwatch-cloud-dry-run Preview the GCS publish and BigQuery load steps." \
		"make portwatch-cloud         Publish PortWatch assets to GCS and load raw.portwatch_monthly." \
		"make portwatch-refresh-cloud Rebuild PortWatch silver, then publish and load it." \
		"make dbt-bigquery-debug      Run dbt debug with env vars derived from Terraform." \
		"make dbt-bigquery-build      Run dbt build with env vars derived from Terraform."

tfvars-init:
	@if [[ -f "$(TFVARS)" ]]; then \
		echo "$(TFVARS) already exists."; \
	else \
		cp "$(TFVARS_EXAMPLE)" "$(TFVARS)"; \
		echo "Created $(TFVARS). Fill in your project, bucket, and IAM values before bootstrap."; \
	fi

check-tfvars:
	@if [[ ! -f "$(TFVARS)" ]]; then \
		echo "Missing $(TFVARS). Run 'make tfvars-init' first."; \
		exit 1; \
	fi

deps-sync:
	uv sync

gcp-auth:
	@if gcloud auth application-default print-access-token >/dev/null 2>&1; then \
		echo "Application Default Credentials already available."; \
	else \
		gcloud auth application-default login; \
	fi

infra-init: check-tfvars
	terraform -chdir=$(TF_DIR) init

infra-plan: check-tfvars infra-init
	terraform -chdir=$(TF_DIR) plan

infra-apply: check-tfvars infra-init
	terraform -chdir=$(TF_DIR) apply

infra-destroy: check-tfvars infra-init
	@python -c 'import json, pathlib, sys; payload = json.loads(pathlib.Path("infra/terraform/terraform.tfvars.json").read_text(encoding="utf-8")); sys.exit(0 if payload.get("allow_force_destroy", False) else "Refusing to destroy because allow_force_destroy is not true in infra/terraform/terraform.tfvars.json.\nSet it to true only when you intentionally want Terraform to delete non-empty buckets and BigQuery datasets.")'
	terraform -chdir=$(TF_DIR) destroy

env-file: check-tfvars
	python $(TF_DIR)/render_dotenv.py > .env
	@echo "Rendered .env from $(TFVARS)."

env-print: check-tfvars
	python $(TF_DIR)/render_dotenv.py

cloud-bootstrap: check-tfvars deps-sync gcp-auth infra-init infra-apply env-file
	@echo "Cloud bootstrap complete."

portwatch-silver:
	uv run python ingest/portwatch/portwatch_silver.py

portwatch-cloud-dry-run: check-tfvars
	uv run python warehouse/publish_portwatch_to_gcs.py --include-auxiliary --dry-run
	uv run python warehouse/load_portwatch_to_bigquery.py --dry-run

portwatch-cloud: check-tfvars
	uv run python warehouse/publish_portwatch_to_gcs.py --include-auxiliary
	uv run python warehouse/load_portwatch_to_bigquery.py

portwatch-refresh-cloud: portwatch-silver portwatch-cloud

dbt-bigquery-debug: check-tfvars
	@eval "$$(python $(TF_DIR)/render_dotenv.py --format export)"; \
	uv run dbt debug --profiles-dir . --target bigquery_dev

dbt-bigquery-build: check-tfvars
	@eval "$$(python $(TF_DIR)/render_dotenv.py --format export)"; \
	uv run dbt build --profiles-dir . --target bigquery_dev
