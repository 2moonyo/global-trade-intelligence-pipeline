SHELL := /bin/bash

PROJECT_ROOT := $(CURDIR)
TF_DIR := infra/terraform
TFVARS := $(TF_DIR)/terraform.tfvars.json
TFVARS_EXAMPLE := $(TF_DIR)/terraform.tfvars.json.example

.DEFAULT_GOAL := help

.PHONY: help tfvars-init check-tfvars deps-sync gcp-auth infra-init infra-plan infra-apply infra-destroy env-file env-print cloud-bootstrap portwatch-extract portwatch-silver portwatch-cloud-dry-run portwatch-cloud portwatch-cloud-dry-run-with-bronze portwatch-cloud-with-bronze portwatch-refresh-cloud comtrade-silver comtrade-routing comtrade-cloud-dry-run comtrade-cloud comtrade-cloud-dry-run-with-bronze comtrade-cloud-with-bronze comtrade-refresh-cloud brent-extract brent-silver brent-cloud-dry-run brent-cloud brent-cloud-dry-run-with-bronze brent-cloud-with-bronze brent-refresh-cloud fx-extract fx-silver fx-cloud-dry-run fx-cloud fx-cloud-dry-run-with-bronze fx-cloud-with-bronze fx-refresh-cloud events-silver events-cloud-dry-run events-cloud events-refresh-cloud dbt-bigquery-debug dbt-bigquery-build dbt-bigquery-docs-generate dbt-bigquery-docs-serve dbt-bigquery-docs-static
.PHONY: portwatch-gcs-dry-run portwatch-gcs portwatch-gcs-dry-run-with-bronze portwatch-gcs-with-bronze
.PHONY: comtrade-gcs-dry-run comtrade-gcs comtrade-gcs-dry-run-with-bronze comtrade-gcs-with-bronze
.PHONY: brent-gcs-dry-run brent-gcs brent-gcs-dry-run-with-bronze brent-gcs-with-bronze
.PHONY: fx-gcs-dry-run fx-gcs fx-gcs-dry-run-with-bronze fx-gcs-with-bronze
.PHONY: events-gcs-dry-run events-gcs
.PHONY: worldbank-energy-extract worldbank-energy-silver worldbank-energy-gcs-dry-run worldbank-energy-gcs worldbank-energy-gcs-dry-run-with-bronze worldbank-energy-gcs-with-bronze worldbank-energy-cloud-dry-run worldbank-energy-cloud worldbank-energy-cloud-dry-run-with-bronze worldbank-energy-cloud-with-bronze worldbank-energy-refresh-cloud

help:
	@printf "%s\n" \
		"make tfvars-init             Copy the Terraform vars example if needed." \
		"make cloud-bootstrap         ADC auth if needed, terraform init/apply, and render .env." \
		"make infra-destroy           Destroy Terraform-managed cloud resources after confirmation." \
		"make portwatch-extract       Run the PortWatch bronze extract with per-run logs and manifest output." \
		"make portwatch-gcs-dry-run   Preview the PortWatch disk-to-GCS publish for silver-only assets." \
		"make portwatch-gcs           Publish PortWatch silver assets from local disk to GCS." \
		"make portwatch-cloud-dry-run Preview the silver-only GCS publish and BigQuery load steps for raw.portwatch_daily/raw.portwatch_monthly." \
		"make portwatch-cloud         Publish silver PortWatch assets to GCS and load raw.portwatch_daily/raw.portwatch_monthly." \
		"make portwatch-gcs-dry-run-with-bronze Preview the PortWatch disk-to-GCS publish including bronze." \
		"make portwatch-gcs-with-bronze Publish PortWatch bronze and silver assets from local disk to GCS." \
		"make portwatch-cloud-dry-run-with-bronze Preview the GCS publish/load steps including bronze." \
		"make portwatch-cloud-with-bronze Publish PortWatch bronze and silver assets, then load raw.portwatch_daily/raw.portwatch_monthly." \
		"make portwatch-refresh-cloud Rebuild PortWatch silver, then publish and load it." \
		"make comtrade-silver        Build canonical Comtrade silver fact slices and dimensions." \
		"make comtrade-routing       Build Comtrade routing outputs from the modular routing package." \
		"make comtrade-gcs-dry-run   Preview the Comtrade disk-to-GCS publish for silver/routing assets." \
		"make comtrade-gcs           Publish Comtrade silver/routing assets from local disk to GCS." \
		"make comtrade-cloud-dry-run Preview the Comtrade silver/routing GCS publish and BigQuery load steps." \
		"make comtrade-cloud         Publish Comtrade silver/routing assets to GCS and load raw.comtrade_*." \
		"make comtrade-gcs-dry-run-with-bronze Preview the Comtrade disk-to-GCS publish including bronze and audit assets." \
		"make comtrade-gcs-with-bronze Publish Comtrade bronze, silver, routing, and audit assets from local disk to GCS." \
		"make comtrade-cloud-dry-run-with-bronze Preview the Comtrade publish/load steps including bronze and audit assets." \
		"make comtrade-cloud-with-bronze Publish Comtrade bronze, silver, routing, and audit assets, then load raw.comtrade_*." \
		"make comtrade-refresh-cloud Rebuild Comtrade silver and routing, then publish and load it." \
		"make brent-extract         Run the Brent bronze extract with per-run logs and manifest output." \
		"make brent-silver          Build partitioned Brent silver daily and monthly parquet outputs." \
		"make brent-gcs-dry-run     Preview the Brent disk-to-GCS publish for silver-only assets." \
		"make brent-gcs             Publish Brent silver assets from local disk to GCS." \
		"make brent-cloud-dry-run   Preview the Brent silver-only GCS publish and BigQuery load steps." \
		"make brent-cloud           Publish Brent silver assets to GCS and load raw.brent_daily/raw.brent_monthly." \
		"make brent-gcs-dry-run-with-bronze Preview the Brent disk-to-GCS publish including bronze." \
		"make brent-gcs-with-bronze Publish Brent bronze and silver assets from local disk to GCS." \
		"make brent-cloud-dry-run-with-bronze Preview the Brent publish/load steps including bronze." \
		"make brent-cloud-with-bronze Publish Brent bronze and silver assets, then load raw.brent_daily/raw.brent_monthly." \
		"make brent-refresh-cloud   Rebuild Brent silver, then publish and load it." \
		"make fx-extract           Run the FX bronze extract using the ECB API." \
		"make fx-silver            Build partitioned FX monthly silver parquet outputs." \
		"make fx-gcs-dry-run       Preview the FX disk-to-GCS publish for silver-only assets." \
		"make fx-gcs               Publish FX silver assets from local disk to GCS." \
		"make fx-cloud-dry-run     Preview the FX silver-only GCS publish and BigQuery load steps." \
		"make fx-cloud             Publish FX silver assets to GCS and load raw.ecb_fx_eu_monthly." \
		"make fx-gcs-dry-run-with-bronze Preview the FX disk-to-GCS publish including bronze." \
		"make fx-gcs-with-bronze   Publish FX bronze and silver assets from local disk to GCS." \
		"make fx-cloud-dry-run-with-bronze Preview the FX publish/load steps including bronze." \
		"make fx-cloud-with-bronze Publish FX bronze and silver assets, then load raw.ecb_fx_eu_monthly." \
		"make fx-refresh-cloud     Rebuild FX silver, then publish and load it." \
		"make events-silver         Build curated event silver outputs from data/bronze/events.csv with run logs." \
		"make events-gcs-dry-run    Preview the events disk-to-GCS publish from local silver outputs." \
		"make events-gcs            Publish events silver assets from local disk to GCS." \
		"make events-cloud-dry-run Preview the events silver GCS publish and BigQuery load steps." \
		"make events-cloud         Publish events silver assets to GCS and load raw.dim_event/raw.bridge_event_*." \
		"make events-refresh-cloud Rebuild event silver, then publish and load it." \
		"make worldbank-energy-extract Run the World Bank energy bronze extract." \
		"make worldbank-energy-silver Build World Bank energy silver annual parquet outputs." \
		"make worldbank-energy-gcs-dry-run Preview the World Bank energy disk-to-GCS publish for silver-only assets." \
		"make worldbank-energy-gcs Publish World Bank energy silver assets from local disk to GCS." \
		"make worldbank-energy-gcs-dry-run-with-bronze Preview the World Bank energy disk-to-GCS publish including bronze." \
		"make worldbank-energy-gcs-with-bronze Publish World Bank energy bronze and silver assets from local disk to GCS." \
		"make worldbank-energy-cloud-dry-run Preview the World Bank energy silver-only GCS publish and BigQuery load steps." \
		"make worldbank-energy-cloud Publish World Bank energy silver assets to GCS and load raw.energy_vulnerability." \
		"make worldbank-energy-cloud-dry-run-with-bronze Preview the World Bank energy publish/load steps including bronze." \
		"make worldbank-energy-cloud-with-bronze Publish World Bank energy bronze and silver assets, then load raw.energy_vulnerability." \
		"make worldbank-energy-refresh-cloud Rebuild World Bank energy silver, then publish and load it." \
		"make dbt-bigquery-debug      Run dbt debug with env vars derived from Terraform." \
		"make dbt-bigquery-build      Run dbt build with env vars derived from Terraform." \
		"make dbt-bigquery-docs-generate Generate dbt docs artifacts into target/." \
		"make dbt-bigquery-docs-serve    Serve dbt docs locally." \
		"make dbt-bigquery-docs-static   Generate a portable static dbt docs HTML file."

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

portwatch-extract:
	PYTHONPATH="$(PROJECT_ROOT)" uv run python ingest/portwatch/portwatch_extract.py

portwatch-silver:
	PYTHONPATH="$(PROJECT_ROOT)" uv run python ingest/portwatch/portwatch_silver.py

portwatch-gcs-dry-run: check-tfvars
	PYTHONPATH="$(PROJECT_ROOT)" uv run python warehouse/publish_portwatch_to_gcs.py --skip-bronze --include-auxiliary --dry-run

portwatch-gcs: check-tfvars
	PYTHONPATH="$(PROJECT_ROOT)" uv run python warehouse/publish_portwatch_to_gcs.py --skip-bronze --include-auxiliary

portwatch-gcs-dry-run-with-bronze: check-tfvars
	PYTHONPATH="$(PROJECT_ROOT)" uv run python warehouse/publish_portwatch_to_gcs.py --include-auxiliary --dry-run

portwatch-gcs-with-bronze: check-tfvars
	PYTHONPATH="$(PROJECT_ROOT)" uv run python warehouse/publish_portwatch_to_gcs.py --include-auxiliary

portwatch-cloud-dry-run: portwatch-gcs-dry-run
	PYTHONPATH="$(PROJECT_ROOT)" uv run python warehouse/load_portwatch_to_bigquery.py --source local --dry-run

portwatch-cloud: portwatch-gcs
	PYTHONPATH="$(PROJECT_ROOT)" uv run python warehouse/load_portwatch_to_bigquery.py

portwatch-cloud-dry-run-with-bronze: portwatch-gcs-dry-run-with-bronze
	PYTHONPATH="$(PROJECT_ROOT)" uv run python warehouse/load_portwatch_to_bigquery.py --source local --dry-run

portwatch-cloud-with-bronze: portwatch-gcs-with-bronze
	PYTHONPATH="$(PROJECT_ROOT)" uv run python warehouse/load_portwatch_to_bigquery.py

portwatch-refresh-cloud: portwatch-silver portwatch-cloud

comtrade-silver:
	PYTHONPATH="$(PROJECT_ROOT)" UV_CACHE_DIR="$(PROJECT_ROOT)/.uv-cache" uv run python ingest/comtrade/comtrade_silver.py

comtrade-routing:
	PYTHONPATH="$(PROJECT_ROOT)" UV_CACHE_DIR="$(PROJECT_ROOT)/.uv-cache" uv run python -m ingest.comtrade.routing

comtrade-gcs-dry-run: check-tfvars
	PYTHONPATH="$(PROJECT_ROOT)" UV_CACHE_DIR="$(PROJECT_ROOT)/.uv-cache" uv run python warehouse/publish_comtrade_to_gcs.py --skip-bronze --dry-run

comtrade-gcs: check-tfvars
	PYTHONPATH="$(PROJECT_ROOT)" UV_CACHE_DIR="$(PROJECT_ROOT)/.uv-cache" uv run python warehouse/publish_comtrade_to_gcs.py --skip-bronze

comtrade-gcs-dry-run-with-bronze: check-tfvars
	PYTHONPATH="$(PROJECT_ROOT)" UV_CACHE_DIR="$(PROJECT_ROOT)/.uv-cache" uv run python warehouse/publish_comtrade_to_gcs.py --dry-run

comtrade-gcs-with-bronze: check-tfvars
	PYTHONPATH="$(PROJECT_ROOT)" UV_CACHE_DIR="$(PROJECT_ROOT)/.uv-cache" uv run python warehouse/publish_comtrade_to_gcs.py

comtrade-cloud-dry-run: comtrade-gcs-dry-run
	PYTHONPATH="$(PROJECT_ROOT)" UV_CACHE_DIR="$(PROJECT_ROOT)/.uv-cache" uv run python warehouse/load_comtrade_to_bigquery.py --source local --dry-run

comtrade-cloud: comtrade-gcs
	PYTHONPATH="$(PROJECT_ROOT)" UV_CACHE_DIR="$(PROJECT_ROOT)/.uv-cache" uv run python warehouse/load_comtrade_to_bigquery.py

comtrade-cloud-dry-run-with-bronze: comtrade-gcs-dry-run-with-bronze
	PYTHONPATH="$(PROJECT_ROOT)" UV_CACHE_DIR="$(PROJECT_ROOT)/.uv-cache" uv run python warehouse/load_comtrade_to_bigquery.py --source local --dry-run

comtrade-cloud-with-bronze: comtrade-gcs-with-bronze
	PYTHONPATH="$(PROJECT_ROOT)" UV_CACHE_DIR="$(PROJECT_ROOT)/.uv-cache" uv run python warehouse/load_comtrade_to_bigquery.py

comtrade-refresh-cloud: comtrade-silver comtrade-routing comtrade-cloud

brent-extract:
	PYTHONPATH="$(PROJECT_ROOT)" UV_CACHE_DIR="$(PROJECT_ROOT)/.uv-cache" uv run python ingest/fred/brent_crude.py

brent-silver:
	PYTHONPATH="$(PROJECT_ROOT)" UV_CACHE_DIR="$(PROJECT_ROOT)/.uv-cache" uv run python ingest/fred/brent_silver.py

brent-gcs-dry-run: check-tfvars
	PYTHONPATH="$(PROJECT_ROOT)" UV_CACHE_DIR="$(PROJECT_ROOT)/.uv-cache" uv run python warehouse/publish_brent_to_gcs.py --skip-bronze --dry-run

brent-gcs: check-tfvars
	PYTHONPATH="$(PROJECT_ROOT)" UV_CACHE_DIR="$(PROJECT_ROOT)/.uv-cache" uv run python warehouse/publish_brent_to_gcs.py --skip-bronze

brent-gcs-dry-run-with-bronze: check-tfvars
	PYTHONPATH="$(PROJECT_ROOT)" UV_CACHE_DIR="$(PROJECT_ROOT)/.uv-cache" uv run python warehouse/publish_brent_to_gcs.py --dry-run

brent-gcs-with-bronze: check-tfvars
	PYTHONPATH="$(PROJECT_ROOT)" UV_CACHE_DIR="$(PROJECT_ROOT)/.uv-cache" uv run python warehouse/publish_brent_to_gcs.py

brent-cloud-dry-run: brent-gcs-dry-run
	PYTHONPATH="$(PROJECT_ROOT)" UV_CACHE_DIR="$(PROJECT_ROOT)/.uv-cache" uv run python warehouse/load_brent_to_bigquery.py --source local --dry-run

brent-cloud: brent-gcs
	PYTHONPATH="$(PROJECT_ROOT)" UV_CACHE_DIR="$(PROJECT_ROOT)/.uv-cache" uv run python warehouse/load_brent_to_bigquery.py

brent-cloud-dry-run-with-bronze: brent-gcs-dry-run-with-bronze
	PYTHONPATH="$(PROJECT_ROOT)" UV_CACHE_DIR="$(PROJECT_ROOT)/.uv-cache" uv run python warehouse/load_brent_to_bigquery.py --source local --dry-run

brent-cloud-with-bronze: brent-gcs-with-bronze
	PYTHONPATH="$(PROJECT_ROOT)" UV_CACHE_DIR="$(PROJECT_ROOT)/.uv-cache" uv run python warehouse/load_brent_to_bigquery.py

brent-refresh-cloud: brent-silver brent-cloud

fx-extract:
	PYTHONPATH="$(PROJECT_ROOT)" UV_CACHE_DIR="$(PROJECT_ROOT)/.uv-cache" uv run python ingest/fred/fx_rates.py

fx-silver:
	PYTHONPATH="$(PROJECT_ROOT)" UV_CACHE_DIR="$(PROJECT_ROOT)/.uv-cache" uv run python ingest/fred/fx_silver.py

fx-gcs-dry-run: check-tfvars
	PYTHONPATH="$(PROJECT_ROOT)" UV_CACHE_DIR="$(PROJECT_ROOT)/.uv-cache" uv run python warehouse/publish_fx_to_gcs.py --skip-bronze --dry-run

fx-gcs: check-tfvars
	PYTHONPATH="$(PROJECT_ROOT)" UV_CACHE_DIR="$(PROJECT_ROOT)/.uv-cache" uv run python warehouse/publish_fx_to_gcs.py --skip-bronze

fx-gcs-dry-run-with-bronze: check-tfvars
	PYTHONPATH="$(PROJECT_ROOT)" UV_CACHE_DIR="$(PROJECT_ROOT)/.uv-cache" uv run python warehouse/publish_fx_to_gcs.py --dry-run

fx-gcs-with-bronze: check-tfvars
	PYTHONPATH="$(PROJECT_ROOT)" UV_CACHE_DIR="$(PROJECT_ROOT)/.uv-cache" uv run python warehouse/publish_fx_to_gcs.py

fx-cloud-dry-run: fx-gcs-dry-run
	PYTHONPATH="$(PROJECT_ROOT)" UV_CACHE_DIR="$(PROJECT_ROOT)/.uv-cache" uv run python warehouse/load_fx_to_bigquery.py --source local --dry-run

fx-cloud: fx-gcs
	PYTHONPATH="$(PROJECT_ROOT)" UV_CACHE_DIR="$(PROJECT_ROOT)/.uv-cache" uv run python warehouse/load_fx_to_bigquery.py

fx-cloud-dry-run-with-bronze: fx-gcs-dry-run-with-bronze
	PYTHONPATH="$(PROJECT_ROOT)" UV_CACHE_DIR="$(PROJECT_ROOT)/.uv-cache" uv run python warehouse/load_fx_to_bigquery.py --source local --dry-run

fx-cloud-with-bronze: fx-gcs-with-bronze
	PYTHONPATH="$(PROJECT_ROOT)" UV_CACHE_DIR="$(PROJECT_ROOT)/.uv-cache" uv run python warehouse/load_fx_to_bigquery.py

fx-refresh-cloud: fx-silver fx-cloud

events-silver:
	PYTHONPATH="$(PROJECT_ROOT)" UV_CACHE_DIR="$(PROJECT_ROOT)/.uv-cache" uv run python ingest/events/events_silver.py

events-gcs-dry-run: check-tfvars
	PYTHONPATH="$(PROJECT_ROOT)" UV_CACHE_DIR="$(PROJECT_ROOT)/.uv-cache" uv run python warehouse/publish_events_to_gcs.py --dry-run

events-gcs: check-tfvars
	PYTHONPATH="$(PROJECT_ROOT)" UV_CACHE_DIR="$(PROJECT_ROOT)/.uv-cache" uv run python warehouse/publish_events_to_gcs.py

events-cloud-dry-run: events-gcs-dry-run
	PYTHONPATH="$(PROJECT_ROOT)" UV_CACHE_DIR="$(PROJECT_ROOT)/.uv-cache" uv run python warehouse/load_events_to_bigquery.py --source local --dry-run

events-cloud: events-gcs
	PYTHONPATH="$(PROJECT_ROOT)" UV_CACHE_DIR="$(PROJECT_ROOT)/.uv-cache" uv run python warehouse/load_events_to_bigquery.py

events-refresh-cloud: events-silver events-cloud

worldbank-energy-extract:
	PYTHONPATH="$(PROJECT_ROOT)" UV_CACHE_DIR="$(PROJECT_ROOT)/.uv-cache" uv run python ingest/world_bank/worldbank_energy.py extract

worldbank-energy-silver:
	PYTHONPATH="$(PROJECT_ROOT)" UV_CACHE_DIR="$(PROJECT_ROOT)/.uv-cache" uv run python ingest/world_bank/worldbank_energy_silver.py

worldbank-energy-gcs-dry-run: check-tfvars
	PYTHONPATH="$(PROJECT_ROOT)" UV_CACHE_DIR="$(PROJECT_ROOT)/.uv-cache" uv run python warehouse/publish_worldbank_energy_to_gcs.py --skip-bronze --dry-run

worldbank-energy-gcs: check-tfvars
	PYTHONPATH="$(PROJECT_ROOT)" UV_CACHE_DIR="$(PROJECT_ROOT)/.uv-cache" uv run python warehouse/publish_worldbank_energy_to_gcs.py --skip-bronze

worldbank-energy-gcs-dry-run-with-bronze: check-tfvars
	PYTHONPATH="$(PROJECT_ROOT)" UV_CACHE_DIR="$(PROJECT_ROOT)/.uv-cache" uv run python warehouse/publish_worldbank_energy_to_gcs.py --dry-run

worldbank-energy-gcs-with-bronze: check-tfvars
	PYTHONPATH="$(PROJECT_ROOT)" UV_CACHE_DIR="$(PROJECT_ROOT)/.uv-cache" uv run python warehouse/publish_worldbank_energy_to_gcs.py

worldbank-energy-cloud-dry-run: worldbank-energy-gcs-dry-run
	PYTHONPATH="$(PROJECT_ROOT)" UV_CACHE_DIR="$(PROJECT_ROOT)/.uv-cache" uv run python warehouse/load_worldbank_energy_to_bigquery.py --source local --dry-run

worldbank-energy-cloud: worldbank-energy-gcs
	PYTHONPATH="$(PROJECT_ROOT)" UV_CACHE_DIR="$(PROJECT_ROOT)/.uv-cache" uv run python warehouse/load_worldbank_energy_to_bigquery.py

worldbank-energy-cloud-dry-run-with-bronze: worldbank-energy-gcs-dry-run-with-bronze
	PYTHONPATH="$(PROJECT_ROOT)" UV_CACHE_DIR="$(PROJECT_ROOT)/.uv-cache" uv run python warehouse/load_worldbank_energy_to_bigquery.py --source local --dry-run

worldbank-energy-cloud-with-bronze: worldbank-energy-gcs-with-bronze
	PYTHONPATH="$(PROJECT_ROOT)" UV_CACHE_DIR="$(PROJECT_ROOT)/.uv-cache" uv run python warehouse/load_worldbank_energy_to_bigquery.py

worldbank-energy-refresh-cloud: worldbank-energy-silver worldbank-energy-cloud

dbt-bigquery-debug: check-tfvars
	@eval "$$(python $(TF_DIR)/render_dotenv.py --format export)"; \
	uv run dbt debug --profiles-dir . --target bigquery_dev

dbt-bigquery-build: check-tfvars
	@eval "$$(python $(TF_DIR)/render_dotenv.py --format export)"; \
	UV_CACHE_DIR="$(PROJECT_ROOT)/.uv-cache" uv run dbt build --profiles-dir . --target bigquery_dev

dbt-bigquery-docs-generate: check-tfvars
	@eval "$$(python $(TF_DIR)/render_dotenv.py --format export)"; \
	UV_CACHE_DIR="$(PROJECT_ROOT)/.uv-cache" uv run dbt docs generate --profiles-dir . --target bigquery_dev

dbt-bigquery-docs-serve: check-tfvars
	@eval "$$(python $(TF_DIR)/render_dotenv.py --format export)"; \
	UV_CACHE_DIR="$(PROJECT_ROOT)/.uv-cache" uv run dbt docs serve --profiles-dir . --target bigquery_dev

dbt-bigquery-docs-static: check-tfvars
	@eval "$$(python $(TF_DIR)/render_dotenv.py --format export)"; \
	UV_CACHE_DIR="$(PROJECT_ROOT)/.uv-cache" uv run dbt docs generate --profiles-dir . --target bigquery_dev --static; \
	echo "Static docs written to target/static_index.html"
