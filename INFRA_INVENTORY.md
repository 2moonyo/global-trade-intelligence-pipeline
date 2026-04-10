# Infra Inventory

## Legacy Cloud Inventory

This is the last known legacy cloud configuration before the account ran out of credit.

| Setting | Legacy value |
| --- | --- |
| Project id | `capfractal` |
| GCP location | `us-central1` |
| GCS bucket | `test-bucket9182` |
| GCS prefix | `cap` |
| BigQuery raw dataset | `raw` |
| BigQuery analytics dataset | `analytics` |

Do not copy these values blindly into the new account. Use them only as a migration reference.

## Terraform Provisioned Resources

Managed by `infra/terraform`:

- one GCS lake bucket
- one BigQuery raw dataset
- one BigQuery analytics dataset
- one optional pipeline service account
- IAM grants for:
  - `roles/storage.objectAdmin`
  - `roles/bigquery.dataEditor`
  - `roles/bigquery.dataViewer`
  - `roles/bigquery.jobUser`

Relevant files:

- `infra/terraform/main.tf`
- `infra/terraform/variables.tf`
- `infra/terraform/outputs.tf`
- `infra/terraform/render_dotenv.py`
- `infra/terraform/terraform.tfvars.json`

## Environment Variables Expected By dbt And Cloud Scripts

Rendered by `infra/terraform/render_dotenv.py`:

- `GCP_PROJECT_ID`
- `GCP_LOCATION`
- `GCS_BUCKET`
- `GCS_PREFIX`
- `GCP_BIGQUERY_RAW_DATASET`
- `GCP_BIGQUERY_ANALYTICS_DATASET`
- `DBT_BIGQUERY_DATASET`

Additional secret env vars seen locally but intentionally not copied here with values:

- `FRED_API_KEY`
- `COMTRADE_API_KEY_DATA`

## dbt Profile Inventory

`profiles.yml` contains:

- `duckdb_dev`
- `bigquery_dev`

Important behavior:

- default profile target is still `duckdb_dev`
- BigQuery target uses OAuth
- BigQuery target resolves project/dataset/location from env vars

## Makefile Entry Points

### Infra

- `make tfvars-init`
- `make cloud-bootstrap`
- `make infra-plan`
- `make infra-apply`
- `make infra-destroy`
- `make env-file`
- `make env-print`

### PortWatch

- `make portwatch-extract`
- `make portwatch-silver`
- `make portwatch-cloud-dry-run`
- `make portwatch-cloud`
- `make portwatch-cloud-dry-run-with-bronze`
- `make portwatch-cloud-with-bronze`
- `make portwatch-refresh-cloud`

### Comtrade

- `make comtrade-silver`
- `make comtrade-routing`
- `make comtrade-cloud-dry-run`
- `make comtrade-cloud`
- `make comtrade-cloud-dry-run-with-bronze`
- `make comtrade-cloud-with-bronze`
- `make comtrade-refresh-cloud`

### Brent

- `make brent-extract`
- `make brent-silver`
- `make brent-cloud-dry-run`
- `make brent-cloud`
- `make brent-cloud-dry-run-with-bronze`
- `make brent-cloud-with-bronze`
- `make brent-refresh-cloud`

### FX

- `make fx-extract`
- `make fx-silver`
- `make fx-cloud-dry-run`
- `make fx-cloud`
- `make fx-cloud-dry-run-with-bronze`
- `make fx-cloud-with-bronze`
- `make fx-refresh-cloud`

### Events

- `make events-silver`
- `make events-cloud-dry-run`
- `make events-cloud`
- `make events-refresh-cloud`

### dbt

- `make dbt-bigquery-debug`
- `make dbt-bigquery-build`
- `make dbt-bigquery-docs-generate`
- `make dbt-bigquery-docs-serve`
- `make dbt-bigquery-docs-static`

## Cloud Publish / Load Script Inventory

### Publish scripts

- `warehouse/publish_portwatch_to_gcs.py`
- `warehouse/publish_comtrade_to_gcs.py`
- `warehouse/publish_brent_to_gcs.py`
- `warehouse/publish_fx_to_gcs.py`
- `warehouse/publish_events_to_gcs.py`
- `warehouse/publish_worldbank_energy_to_gcs.py`

### Load scripts

- `warehouse/load_portwatch_to_bigquery.py`
- `warehouse/load_comtrade_to_bigquery.py`
- `warehouse/load_brent_to_bigquery.py`
- `warehouse/load_fx_to_bigquery.py`
- `warehouse/load_events_to_bigquery.py`
- `warehouse/load_worldbank_energy_to_bigquery.py`

### Shared infra helpers

- `warehouse/bigquery_load_state.py`
- `warehouse/gcs_publish_common.py`
- `ingest/common/cloud_config.py`
- `ingest/common/gcs_io.py`
- `ingest/common/run_artifacts.py`

## Local Data Contract Inventory

### Bronze

Examples:

- `data/bronze/portwatch`
- bronze Brent extracts
- bronze Comtrade extracts
- bronze events CSV

### Silver

Silver contract folders currently present:

- `data/silver/brent/brent_daily`
- `data/silver/brent/brent_monthly`
- `data/silver/comtrade/comtrade_fact`
- `data/silver/comtrade/dimensions`
- `data/silver/events/bridge_event_month_chokepoint_core`
- `data/silver/events/bridge_event_month_maritime_region`
- `data/silver/fx/ecb_fx_eu_monthly`
- `data/silver/portwatch/portwatch_daily`
- `data/silver/portwatch/portwatch_monthly`
- `data/silver/portwatch/dimensions`
- `data/silver/worldbank_energy/energy_vulnerability`

## BigQuery Raw Table Inventory Expected After Migration

These are the raw tables declared in `models/sources/silver_sources.yml` and expected in the migrated `raw` dataset:

- `comtrade_fact`
- `dim_country`
- `dim_time`
- `dim_commodity`
- `dim_trade_flow`
- `dim_chokepoint`
- `dim_country_ports`
- `route_applicability`
- `dim_trade_routes`
- `chokepoint_bridge`
- `bridge_event_month_chokepoint_core`
- `bridge_event_month_maritime_region`
- `dim_event`
- `portwatch_daily`
- `portwatch_monthly`
- `brent_daily`
- `brent_monthly`
- `ecb_fx_eu_monthly`
- `energy_vulnerability`

If any of these are missing, the full dbt semantic build will not complete cleanly.

## Logging And Manifest Inventory

The repo stores operational state heavily in JSONL manifests and logs.

Important directories:

- `logs/portwatch`
- `logs/comtrade`
- `logs/brent`
- `logs/events`
- `logs/fx`
- `logs/worldbank_energy`
- `logs/dbt.log`

These logs are useful for migration auditing, but they are not the source of truth for semantic logic.

## Migration-Specific Infra Notes

### Required auth

- Application Default Credentials are expected for local work
- dbt BigQuery target uses OAuth

### Location discipline

Keep these aligned:

- bucket location
- BigQuery dataset location
- dbt target location

The previous account used `us-central1`.

### Secrets

Recreate API secrets intentionally in the new environment.

Do not commit or share live secret values in handover docs.

### Recommended migration order

1. Terraform bootstrap
2. Render env vars
3. Recreate raw landings
4. Rebuild dbt
5. Reconnect Looker

## Important Operational Footnotes

- `portwatch_daily` is required for the Page 2 daily marts.
- The shared `safe_divide` macro was fixed locally and must be included in the new deployment.
- Some paths in the repo still mention DuckDB or Streamlit, but those are not the production path for the dashboard.
- The root handover markdown files should be copied with the repo and treated as part of the migration artifact set.
