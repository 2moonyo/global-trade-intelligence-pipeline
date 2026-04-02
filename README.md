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

## Warehouse shape

The live warehouse is organized as:

1. filesystem data assets in `data/bronze/*` and `data/silver/*`
2. a `raw` landing schema loaded by `warehouse/bootstrap_silver_to_duckdb.sql`
3. dbt staging models in `analytics_staging` and `analytics_analytics_staging`
4. dbt marts in `analytics_marts` and `analytics_analytics_marts`

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

The event area still materializes into `analytics_analytics_staging` and `analytics_analytics_marts`, while the main marts live in `analytics_marts`.

The current event silver contract is now built from `data/bronze/events.csv` by `ingest/events/events_silver.py`, which writes:

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

- The BigQuery profile is ready, but a number of models still contain DuckDB-specific SQL such as `strptime`, `strftime`, `generate_series`, `varchar`, and `double`.
- That means the profile swap is scaffolded, but the full dbt project will still need SQL adapter refactors before a successful BigQuery build.

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
