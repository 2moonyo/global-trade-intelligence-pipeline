# DuckDB + dbt Analytics Warehouse

This repository contains the implemented DuckDB warehouse and dbt project for the capstone analytics stack. The current build is larger than the original bootstrap: it includes core trade facts, route and hub enrichments, macro and energy marts, event impact modelling, canonical-grain provenance, a dbt-generated time dimension, and a generic event location layer.

The repo now also includes first-pass cloud paths for the PortWatch, Comtrade, Brent, and events vertical slices:

1. publish local PortWatch bronze and silver parquet assets to GCS
2. load `silver/portwatch/portwatch_monthly` into BigQuery `raw.portwatch_monthly`
3. build canonical Comtrade silver fact slices, dimensions, and routing outputs
4. publish Comtrade silver and routing assets to GCS
5. load `raw.comtrade_fact` plus the supporting Comtrade dimensions and route tables into BigQuery
6. publish Brent silver assets to GCS
7. load `raw.brent_daily` and `raw.brent_monthly` into BigQuery
8. publish events silver parquet assets to GCS
9. load `raw.dim_event`, `raw.bridge_event_month_chokepoint_core`, and `raw.bridge_event_month_maritime_region` into BigQuery
10. point dbt at either DuckDB or BigQuery via `--target`

## First-Time GCP VM Setup

Use this path for a fresh GCP project, or for a deliberate destroy/recreate test in a disposable project.

The setup has two phases:

1. local laptop bootstrap creates cloud infrastructure, pushes approved secrets to Secret Manager, copies the repo to the VM, renders `/etc/capstone/pipeline.env`, and starts Docker Compose
2. VM bootstrap runs the full data pipeline in dependency order

### 1. Prepare GCP auth and APIs

Use an isolated `gcloud` configuration so you do not accidentally deploy to the old project:

```bash
gcloud auth login
gcloud config configurations create capstone-new-account
gcloud config configurations activate capstone-new-account
gcloud config set account YOUR_EMAIL
gcloud config set project NEW_PROJECT_ID
gcloud auth application-default login
gcloud auth application-default set-quota-project NEW_PROJECT_ID
```

Enable the APIs Terraform and the VM runtime need:

```bash
gcloud services enable \
  serviceusage.googleapis.com \
  compute.googleapis.com \
  iam.googleapis.com \
  secretmanager.googleapis.com \
  storage.googleapis.com \
  bigquery.googleapis.com
```

### 2. Fill local config and secrets

Create and edit Terraform inputs:

```bash
make tfvars-init
```

At minimum, set:

- `project_id`
- `gcp_location`
- `primary_region`
- `primary_zone`
- `gcs_bucket_name`
- IAM member lists for your user/service accounts

For a Europe-first fresh VM, use:

```json
{
  "gcp_location": "europe-west1",
  "primary_region": "europe-west1",
  "primary_zone": "europe-west1-b",
  "legacy_compute_vm_enabled": false,
  "primary_compute_vm_enabled": true,
  "primary_boot_restore_from_snapshot": false,
  "primary_data_restore_from_snapshot": false,
  "recovery_boot_disk_enabled": false,
  "recovery_data_disk_enabled": false,
  "recovery_vm_enabled": false
}
```

Create `.env` and add the approved secret values that will seed Secret Manager:

```bash
./scripts/bootstrap_local.sh
```

Fill these values in `.env`:

- `FRED_API_KEY`
- `COMTRADE_API_KEY_DATA`
- `COMTRADE_API_KEY_DATA_A`
- `COMTRADE_API_KEY_DATA_B`
- `POSTGRES_USER`
- `POSTGRES_PASSWORD`
- `POSTGRES_DB`
- `POSTGRES_SCHEMA`

The VM does not need a local JSON key. Keep `GOOGLE_APPLICATION_CREDENTIALS=` empty for the VM path. GCP auth on the VM uses the attached service account through metadata ADC, while API/Postgres secrets are pulled from Secret Manager into `/etc/capstone/pipeline.env`.

### 3. Create the VM and runtime stack

From your laptop:

```bash
make vm-bootstrap VM_BOOTSTRAP_ARGS="--reset-known-host --show-resolved"
```

This command:

- applies Terraform
- syncs approved `.env` secrets to Secret Manager
- resolves the VM external IP and Linux user
- copies the current repo to `/var/lib/pipeline/capstone`
- renders `/etc/capstone/pipeline.env` from Terraform config plus Secret Manager
- starts/restarts `capstone-stack`

The `--reset-known-host` flag is useful for destroy/recreate testing because recreated VMs can reuse an IP with a new SSH host key.

### 4. Run the full first-time pipeline on the VM

SSH to the VM:

```bash
gcloud compute ssh capstone-vm-eu --zone europe-west1-b
```

Then run the full bootstrap sequence:

```bash
cd /var/lib/pipeline/capstone
./scripts/vm_batches/run_full_bootstrap.sh
```

That script runs, in order:

- non-Comtrade phase 1
- non-Comtrade phase 2
- Comtrade day 1 through day 6
- World Bank energy after Comtrade day 6 has completed

If a batch fails, resume the failed set instead of restarting everything:

```bash
cd /var/lib/pipeline/capstone
./scripts/vm_batches/run_set.sh comtrade-day-2 --start-at-task silver
```

If you need to refresh VM secrets from Secret Manager before a rerun:

```bash
cd /var/lib/pipeline/capstone
SYNC_SECRETS_BEFORE_RUN=true SECRET_PROJECT_ID=NEW_PROJECT_ID \
./scripts/vm_batches/run_set.sh comtrade-day-2
```

### 5. Enable timers only after the first bootstrap succeeds

```bash
sudo systemctl enable --now capstone-schedule-lane-incremental_daily.timer
sudo systemctl enable --now capstone-schedule-lane-weekly_refresh.timer
sudo systemctl enable --now capstone-schedule-lane-monthly_refresh.timer
sudo systemctl enable --now capstone-schedule-lane-yearly_refresh.timer
systemctl list-timers 'capstone-schedule-lane-*'
```

### Destroy/recreate smoke test

Only use full destroy in a disposable project. It can delete Terraform-managed buckets, BigQuery datasets, secrets, VM disks, and runtime state.

First set this in `infra/terraform/terraform.tfvars.json`:

```json
"allow_force_destroy": true
```

Then run:

```bash
make infra-destroy
make vm-bootstrap VM_BOOTSTRAP_ARGS="--reset-known-host --show-resolved"
```

SSH to the new VM and run:

```bash
cd /var/lib/pipeline/capstone
./scripts/vm_batches/run_full_bootstrap.sh
```

For a VM-only reset that keeps bucket, datasets, and Secret Manager resources, prefer:

```bash
make infra-destroy-vm
make vm-bootstrap VM_BOOTSTRAP_ARGS="--reset-known-host --show-resolved"
```

## Terraform-First Quick Start

Once you have filled in `infra/terraform/terraform.tfvars.json`, the intended flow is:

```bash
make cloud-bootstrap
make portwatch-refresh-cloud
make comtrade-refresh-cloud
make brent-refresh-cloud
make events-refresh-cloud
```

Or, if you want to preview the cloud publish/load before writing anything:

```bash
make cloud-bootstrap
make portwatch-cloud-dry-run
make comtrade-cloud-dry-run
make brent-cloud-dry-run
make events-cloud-dry-run
```

If you later want to tear the Terraform-managed cloud resources back down, set `"allow_force_destroy": true` in `infra/terraform/terraform.tfvars.json` and then run:

```bash
make infra-destroy
```

## Execution Profiles: VM Only vs Hybrid

The default execution profile is `all_vm`. It preserves the current VM-first baseline: systemd timers on the VM own all scheduled datasets, and existing VM batch wrappers continue to work.

The additive hybrid profile is `hybrid_vm_serverless`:

- Comtrade stays on the VM.
- PortWatch, Brent, FX, Events, and World Bank Energy scheduled refreshes run as Cloud Run Jobs.
- GCS prefixes, BigQuery raw tables, dbt models, marts, and dashboard-facing outputs stay the same.
- Cloud Run uses service-account ADC and Secret Manager env injection; no JSON key is required.

Execution ownership lives in `ops/execution_profiles.json`. The VM queue runner reads `EXECUTION_PROFILE` and `EXECUTION_RUNTIME`; manual VM dataset wrappers still call the dataset batch runner directly for operator recovery.

For a safe hybrid rollout:

```bash
# 1. Prove all_vm remains the active baseline.
terraform -chdir=infra/terraform plan -var='execution_profile=all_vm'

# 2. Build/push the pipeline image for Cloud Run.
export IMAGE_URI=REGION-docker.pkg.dev/PROJECT/REPOSITORY/capstone-pipeline:TAG
gcloud auth configure-docker REGION-docker.pkg.dev
docker buildx build --platform linux/amd64 -f docker/pipeline/Dockerfile -t "$IMAGE_URI" --push .

# 3. Preview hybrid resources. Keep Scheduler paused for first rollout.
terraform -chdir=infra/terraform plan \
  -var='execution_profile=hybrid_vm_serverless' \
  -var="serverless_container_image=$IMAGE_URI" \
  -var='serverless_scheduler_paused=true'
```

After applying hybrid infrastructure, render the VM env with `EXECUTION_PROFILE=hybrid_vm_serverless`, validate that VM scheduled queues skip serverless-owned non-Comtrade batches, execute one Cloud Run Job manually, then unpause Cloud Scheduler. The full checklist is in `docs/hybrid-vm-serverless-rollout-plan.md`.

## VM operations quick links

For VM runtime setup and operations, see `ops/vm/README.md`.

For out-of-schedule, on-demand batch set execution on the VM (including per-Comtrade-day scripts and non-Comtrade phase 1/2 scripts), see the "On-demand batch sets (outside schedule timers)" section in `ops/vm/README.md`.

For the full local-edit -> git push -> VM pull -> docker rebuild -> manual run lifecycle, see the "End-to-end operator workflow: edit, push, pull, rebuild, run" section in `ops/vm/README.md`.

Connection variable discovery (`VM_HOST`, `VM_USER`, `SSH_KEY_PATH`) is documented in the “How to find VM_HOST, VM_USER, and SSH_KEY_PATH” section in `ops/vm/README.md`.

Two helper scripts are available under `scripts/`:

- `scripts/vm_repo_sync.sh`: repository sync only (initialize/pull/update branch on VM)
- `scripts/vm_api_insert.sh`: insert or update runtime API keys in `/etc/capstone/pipeline.env`

Typical workflow from laptop:

```bash
scripts/vm_repo_sync.sh \
  --vm-user chromazone \
  --vm-host 104.199.42.249 \
  --ssh-key-path "$HOME/.ssh/google_compute_engine" \
  --vm-repo-dir /var/lib/pipeline/capstone \
  --repo-url git@github.com:OWNER/REPO.git \
  --branch cloud_migration

scripts/vm_api_insert.sh \
  --vm-user chromazone \
  --vm-host 104.199.42.249 \
  --ssh-key-path "$HOME/.ssh/google_compute_engine" \
  --interactive-comtrade \
  --interactive-fred \
  --show-keys
```

To pin the VM repo to a specific commit instead of latest branch head:

```bash
scripts/vm_repo_sync.sh \
  --vm-user chromazone \
  --vm-host 104.199.42.249 \
  --ssh-key-path "$HOME/.ssh/google_compute_engine" \
  --repo-url git@github.com:OWNER/REPO.git \
  --branch cloud_migration \
  --commit 0123abcd4567ef89deadbeefcafefeed12345678
```

## Warehouse shape

The live warehouse is organized as:

1. filesystem data assets in `data/bronze/*` and `data/silver/*`
2. a `raw` landing schema loaded by `warehouse/bootstrap_silver_to_duckdb.sql`
3. dbt staging models in `analytics_staging`
4. dbt marts in `analytics_marts`

This is not a strict in-database medallion layout. In practice:

- trade, routing, PortWatch, Brent, and event raw tables are loaded mostly from curated silver outputs
- ECB FX raw tables are loaded from bronze CSV batches, while World Bank energy now lands through annual silver parquet partitions
- `raw` is therefore a mixed landing layer, not a pure bronze mirror

## Core model families

### Trade and routing

- `stg_comtrade_trade_base`
- `stg_comtrade_fact`
- `fct_reporter_partner_commodity_month`
- `fct_reporter_partner_commodity_route_month`
- `fct_reporter_partner_commodity_hub_month`
- `fct_reporter_partner_commodity_month_provenance`

The canonical trade grain is reporter + partner + commodity + month + flow. The route fact preserves that grain, the hub fact expands it in an allocation-safe way, and the provenance fact stores canonical-grain lineage back to the analytical rows in `raw.comtrade_fact`.

### Conformed staging dimensions

- `stg_dim_country`
- `stg_dim_time`
- `stg_dim_commodity`
- `stg_dim_trade_flow`

`stg_dim_time` is now generated in dbt from observed months across trade, PortWatch, Brent, FX, energy, and event sources, with configurable lead and lag buffers. It is no longer just a raw pass-through.

### Exposure, macro, and energy marts

- `mart_reporter_month_trade_summary`
- `mart_reporter_commodity_month_trade_summary`
- `mart_trade_exposure`
- `mart_reporter_month_chokepoint_exposure`
- `mart_reporter_month_chokepoint_exposure_with_brent`
- `mart_hub_dependency_month`
- `mart_macro_monthly_features`
- `mart_reporter_month_macro_features`
- `mart_reporter_energy_vulnerability`

### Event models

- `stg_event_raw`
- `stg_event_month_chokepoint`
- `stg_event_month_region`
- `stg_event_location`
- `dim_event`
- `dim_location`
- `bridge_event_month`
- `bridge_event_chokepoint`
- `bridge_event_region`
- `bridge_event_location`
- `mart_event_impact`

The event area materializes alongside the rest of the project under the same dbt-managed staging and marts schemas.

The current event silver contract is built by `ingest/events/events_silver.py` using this source precedence:

- `--events-csv-path` CLI argument (when supplied)
- `EVENTS_SEED_CSV_PATH` environment variable (when set)
- `data/seed/events/events_seed.csv` (preferred durable location)
- `data/bronze/events.csv` (legacy fallback)

It then writes:

- `logs/events/events_silver.log`
- `logs/events/events_silver_manifest.jsonl`
- `logs/events/publish_events_to_gcs.log`
- `logs/events/publish_events_to_gcs_manifest.jsonl`
- `logs/events/load_events_to_bigquery.log`
- `logs/events/load_events_to_bigquery_manifest.jsonl`
- `data/silver/events/dim_event.csv`
- `data/silver/events/dim_event.parquet`
- `data/silver/events/bridge_event_month_chokepoint_core.csv`
- `data/silver/events/bridge_event_month_maritime_region.csv`
- partitioned parquet bridge outputs under `data/silver/events/bridge_event_month_*`

## Build and refresh

### 1. Load raw tables into DuckDB

Run from project root:

```bash
make events-silver
uv run python warehouse/load_silver_to_duckdb.py
```

This creates or updates `warehouse/analytics.duckdb` and refreshes the `raw.*` tables.

### 2. Build dbt models

Install or sync dependencies if needed:

```bash
uv sync
```

Run a full build:

```bash
uv run dbt build --profiles-dir .
```

Or run targeted areas:

```bash
uv run dbt run --profiles-dir . --target duckdb_dev --select staging
uv run dbt run --profiles-dir . --target duckdb_dev --select marts
uv run dbt test --profiles-dir . --target duckdb_dev
```

### 3. Publish the PortWatch slice to GCS

### 2a. Publish and load the events slice

Preview the upload and BigQuery load plan:

```bash
make events-cloud-dry-run
```

Run the full events cloud slice:

```bash
make events-refresh-cloud
```

Or call the commands directly:

```bash
uv run python warehouse/publish_events_to_gcs.py
uv run python warehouse/load_events_to_bigquery.py
```

Populate the cloud settings in one of two ways:

- local `.env` copied from `.env.example`
- `infra/terraform/terraform.tfvars.json` if you are using the Terraform scaffold

For local development, prefer Application Default Credentials instead of a service-account key:

```bash
gcloud auth application-default login
```

Then publish the PortWatch assets:

```bash
uv run python warehouse/publish_portwatch_to_gcs.py --include-auxiliary
```

Preview the upload plan without calling GCS:

```bash
uv run python warehouse/publish_portwatch_to_gcs.py --include-auxiliary --dry-run
```

### 3a. Run the PortWatch extract with per-run logs

The bronze extract now writes a rolling log and a JSONL manifest entry on every run:

- `logs/portwatch/portwatch_extract.log`
- `logs/portwatch/portwatch_extract_manifest.jsonl`

Each manifest row captures:

- requested start and end dates
- selected chokepoints and derived region fields
- processed dates, dates with rows, and null dates
- per-day row counts and elapsed seconds
- monthly row-count summaries
- files written and total extracted rows
- run duration and failure summary if the run errors

Run it directly:

```bash
uv run python ingest/portwatch/portwatch_extract.py --start-date 2026-01-01 --end-date 2026-01-31
```

Or via `make`:

```bash
make portwatch-extract
```

### 4. Load PortWatch monthly into BigQuery

After the canonical monthly silver partitions are in GCS:

```bash
uv run python warehouse/load_portwatch_to_bigquery.py
```

Preview the touched month partitions and GCS URIs first:

```bash
uv run python warehouse/load_portwatch_to_bigquery.py --dry-run
```

The loader replaces only the touched `month_start_date` partitions by default. Use `--append-only` only for controlled one-off loads.

### 4a. Makefile workflow

The repo now includes a root `Makefile` so the terraform-first flow is mostly two commands after you fill in the tfvars file:

```bash
make tfvars-init
make cloud-bootstrap
make portwatch-cloud-dry-run
make portwatch-refresh-cloud
make comtrade-cloud-dry-run
make comtrade-refresh-cloud
```

Useful targets:

- `make cloud-bootstrap`
- `make infra-destroy`
- `make portwatch-extract`
- `make portwatch-cloud-dry-run`
- `make portwatch-cloud`
- `make portwatch-refresh-cloud`
- `make comtrade-silver`
- `make comtrade-routing`
- `make comtrade-cloud-dry-run`
- `make comtrade-cloud`
- `make comtrade-refresh-cloud`
- `make dbt-bigquery-debug`
- `make dbt-bigquery-build`

### 4b. Comtrade silver and routing

The scripted Comtrade path now mirrors the repo's other vertical slices:

- `ingest/comtrade/comtrade_silver.py` builds canonical month-level silver fact slices plus `dim_country`, `dim_time`, `dim_commodity`, and `dim_trade_flow`
- `ingest/comtrade/comtrade_routing.py` executes the authoritative `05_comtrade_silver_enrichment_scenario_graph_routing_v4.ipynb` logic as a script and writes the routing outputs separately from the base silver fact
- `warehouse/publish_comtrade_to_gcs.py` publishes Comtrade bronze, silver, routing, metadata, and per-run audit artifacts
- `warehouse/load_comtrade_to_bigquery.py` loads `raw.comtrade_fact`, `raw.dim_country`, `raw.dim_time`, `raw.dim_commodity`, `raw.dim_trade_flow`, `raw.route_applicability`, and `raw.dim_trade_routes`

The Comtrade fact is physically stored by month slice:

- `data/silver/comtrade/comtrade_fact/year=YYYY/month=MM/reporter_iso3=ISO3/cmd_code=XXXX/flow_code=M/comtrade_fact.parquet`

Within each slice the base-row dedupe grain keeps the operational fields that were needed to eliminate false duplicates:

- `period`
- `reporter_iso3`
- `partner_iso3`
- `flowCode`
- `cmdCode`
- `customsCode`
- `motCode`
- `partner2Code`

The silver builder overwrites only touched slices and skips unchanged files by fingerprint, so reruns avoid unnecessary local rewrites, GCS uploads, and BigQuery reloads.

Run the scripted silver builder directly:

```bash
uv run python ingest/comtrade/comtrade_silver.py --since-period 202401 --until-period 202412
```

Run the routing build with a local Natural Earth cache, or let it bootstrap that cache from GCS:

```bash
uv run python ingest/comtrade/comtrade_routing.py \
  --natural-earth-path data/reference/geography/ne_110m_admin_0_countries.zip \
  --natural-earth-gcs-uri gs://YOUR_BUCKET/reference/geography/ne_110m_admin_0_countries.zip
```

Each run writes rolling logs plus run-linked audit artifacts under `data/metadata/comtrade/ingest_reports/run_id=<run_id>/`.

### 5. BigQuery dbt target

The profile now supports both local DuckDB and BigQuery:

```bash
uv run dbt debug --profiles-dir . --target bigquery_dev
uv run dbt build --profiles-dir . --target bigquery_dev
```

If you are using Terraform as the source of truth for names, you can generate `.env` from your Terraform vars file:

```bash
python infra/terraform/render_dotenv.py > .env
```

Or let the `Makefile` inject the Terraform-derived values directly for dbt:

```bash
make dbt-bigquery-debug
make dbt-bigquery-build
```

Current limitation:

- The BigQuery profile is active and works for targeted validation runs such as selected marts.
- Full-project BigQuery parity should still be treated as an ongoing migration task and validated model by model.

## Useful dbt vars

`stg_dim_time` supports buffered calendar generation:

```bash
uv run dbt run --profiles-dir . --select stg_dim_time --vars '{"dim_time_lag_months": 12, "dim_time_lead_months": 12}'
```

Default behavior already uses a 12-month lag and 12-month lead.

## Current implementation notes

- `canonical_grain_key` is carried through staged and fact trade models to support downstream lineage joins.
- `fct_reporter_partner_commodity_month_provenance` is the canonical-grain provenance table for auditability and future ingest-log integration.
- PortWatch exposure and event impact both use dbt-derived chokepoint stress signals: exposure marts consume the expanding `stress_index` family, while event impact consumes the expanding component z-scores exposed through `stg_chokepoint_stress_zscore`.
- The event location layer now separates generic location semantics from chokepoint-only logic. Legacy raw event input still includes a column named `chokepoint_name` for some non-core locations.

## Query examples

```sql
-- canonical trade fact
select *
from analytics_marts.fct_reporter_partner_commodity_month
order by trade_value_usd desc
limit 20;

-- canonical-grain provenance for drill-back into source batches/files
select *
from analytics_marts.fct_reporter_partner_commodity_month_provenance
order by raw_row_count desc
limit 20;

-- reporter-level chokepoint exposure
select *
from analytics_marts.mart_reporter_month_chokepoint_exposure
order by chokepoint_trade_exposure_ratio desc nulls last
limit 20;

-- event impact with generic locations available through bridge tables
select *
from analytics_marts.mart_event_impact
order by event_start_month desc
limit 20;
```

## Streamlit dashboard

The repository includes a production-minded Streamlit frontend under `app/` with five narrative pages:

- Executive Overview
- Trade Dependence
- Chokepoint Stress & Exposure
- Events & Commodity Impact
- Energy Vulnerability Context

The app reads DuckDB in read-only mode, caches the connection with `st.cache_resource`, caches query results with `st.cache_data`, uses parameterised SQL throughout, and degrades gracefully when optional fact or event tables are missing.

### Local run

Install the frontend dependencies:

```bash
python -m pip install -r requirements-streamlit.txt
```

Or with `uv`:

```bash
uv venv
source .venv/bin/activate
uv pip install -r requirements-streamlit.txt
```

Run the dashboard from project root:

```bash
streamlit run app/streamlit_app.py
```

If you want to point the app at a different DuckDB file, set `TRADE_DUCKDB_PATH` first:

```bash
export TRADE_DUCKDB_PATH=/path/to/analytics.duckdb
streamlit run app/streamlit_app.py
```

### Docker run

Build the image from project root:

```bash
docker build -f docker/streamlit/Dockerfile -t capstone-streamlit .
```

Run the container:

```bash
docker run --rm -p 8501:8501 capstone-streamlit
```

Then open `http://localhost:8501`.

### Dashboard notes

- Reporter filters are limited to the countries that actually appear in the trade marts.
- The overview and dependence pages prioritise dbt marts and only fall back to lower-grain facts where the selected filters require that grain.
- Chokepoint traffic stress comes from `analytics_staging.stg_portwatch_stress_metrics`, derived from the raw PortWatch monthly fact, so it covers fewer chokepoints than the exposure marts.
- The events page uses true event bridge tables when available and falls back to commodity and traffic trends when they are not.
