# Run Instructions

This file is the practical cutover runbook for moving the project to a new GCP account and restoring the dbt + BigQuery + Looker Studio workflow.

Use this together with:

1. `PROJECT_STATE.md`
2. `ARCHITECTURE.md`
3. `INFRA_INVENTORY.md`
4. `DASHBOARD_STATUS.md`
5. `DASHBOARD_FIELD_MAP.md`
6. `NEXT_STEPS.md`
7. `CODEX_HANDOVER_PROMPT.md`

## Goal

Restore the project in a new GCP account with the least risky sequence:

1. provision infra
2. recreate raw landings
3. rebuild dbt in BigQuery
4. validate Page 1 and Page 2 first
5. reconnect Looker
6. then continue with Page 3 and Page 4

## Before You Start

Assumptions:

- you are in the repo root
- you have access to the new GCP project
- billing is enabled on the new project
- Application Default Credentials can be created locally
- API secrets such as `FRED_API_KEY` and `COMTRADE_API_KEY_DATA` will be re-added in the new environment

Important reminders:

- `profiles.yml` defaults to `duckdb_dev`, so use the BigQuery commands below
- the production dashboard path is BigQuery + dbt + Looker Studio
- DuckDB and Streamlit assets are legacy and should not drive the migration
- only Page 1 and Page 2 were screenshot-confirmed in the last Looker freeze

## Step 1: Prepare The New Terraform Inputs

Copy the example if needed:

```bash
make tfvars-init
```

Edit:

- `infra/terraform/terraform.tfvars.json`

Set at minimum:

- new `project_id`
- new `gcp_location`
- new `gcs_bucket_name`
- IAM members for BigQuery and GCS

Recommended:

- keep one consistent location for bucket, datasets, and dbt target
- use clear names for the new bucket and datasets

## Step 2: Authenticate And Bootstrap Cloud Infra

Run:

```bash
make cloud-bootstrap
```

This handles:

- dependency sync
- ADC auth if needed
- Terraform init/apply
- `.env` rendering from Terraform values

If you want to inspect first:

```bash
make infra-plan
make infra-apply
make env-file
```

Checkpoint:

- bucket exists
- `raw` dataset exists
- `analytics` dataset exists
- `.env` contains the new project values

## Step 3: Verify BigQuery dbt Connectivity

Run:

```bash
make dbt-bigquery-debug
```

If that passes, dbt can see the new project with the Terraform-derived env vars.

## Step 4: Recreate Raw Data Landings

Recommended refresh order:

1. PortWatch
2. Brent
3. Comtrade
4. FX
5. Events
6. World Bank energy

Run:

```bash
make portwatch-refresh-cloud
make brent-refresh-cloud
make comtrade-refresh-cloud
make fx-refresh-cloud
make events-refresh-cloud
```

If you want dry runs first:

```bash
make portwatch-cloud-dry-run
make brent-cloud-dry-run
make comtrade-cloud-dry-run
make fx-cloud-dry-run
make events-cloud-dry-run
```

Checkpoint in BigQuery `raw`:

- `portwatch_daily`
- `portwatch_monthly`
- `brent_daily`
- `brent_monthly`
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
- `ecb_fx_eu_monthly`
- `energy_vulnerability`

If any of those are missing, stop and fix the raw load before running the full dbt build.

## Step 5: Run The Full dbt Build In BigQuery

Run:

```bash
make dbt-bigquery-build
```

If you want a phased build:

```bash
eval "$(python infra/terraform/render_dotenv.py --format export)"
UV_CACHE_DIR="$PWD/.uv-cache" uv run dbt build --profiles-dir . --target bigquery_dev --select models/staging
UV_CACHE_DIR="$PWD/.uv-cache" uv run dbt build --profiles-dir . --target bigquery_dev --select models/marts/dimensions
UV_CACHE_DIR="$PWD/.uv-cache" uv run dbt build --profiles-dir . --target bigquery_dev --select models/marts
UV_CACHE_DIR="$PWD/.uv-cache" uv run dbt build --profiles-dir . --target bigquery_dev --select models/marts/semantics
```

## Step 6: Validate The Daily Layer Explicitly

This is important because the last known Page 2 issue was fixed late in the project.

Run:

```bash
eval "$(python infra/terraform/render_dotenv.py --format export)"
UV_CACHE_DIR="$PWD/.uv-cache" uv run dbt build --profiles-dir . --target bigquery_dev --select +mart_global_daily_market_signal +mart_chokepoint_daily_signal
```

Expected result:

- `stg_portwatch_daily` builds
- `stg_brent_daily` builds
- `mart_chokepoint_daily_signal` builds
- `mart_global_daily_market_signal` builds
- `tests/stg_portwatch_daily_share_bounds.sql` passes

Known historical issue:

- a precedence bug in `macros/fx_transform.sql` caused malformed `safe_divide` output
- that fix exists in the repo and must be included in the migrated deployment

## Step 7: Restore Page 1 And Page 2 First

Use the screenshot freeze and the field map as the restore target.

Frozen artifacts:

- Page 1: `/Users/chromazone/Documents/Python Data Enginering Zoomcamp/Capstone Data Tests/Data State 10:04:2026/Looker/Screenshot 2026-04-10 at 20.40.24.png`
- Data source manager: `/Users/chromazone/Documents/Python Data Enginering Zoomcamp/Capstone Data Tests/Data State 10:04:2026/Looker/Screenshot 2026-04-10 at 20.43.00.png`
- Page 2: `/Users/chromazone/Documents/Python Data Enginering Zoomcamp/Capstone Data Tests/Data State 10:04:2026/Looker/Screenshot 2026-04-10 at 20.45.38.png`

Reconnect these Looker data sources first:

- `mart_dashboard_global_trade_overview`
- `mart_trade_month_coverage_status`
- `mart_executive_monthly_system_snapshot`
- `mart_chokepoint_monthly_stress_detail`
- `mart_global_daily_market_signal`
- `mart_chokepoint_daily_signal`

Recommended rebuild order in Looker:

1. Page 1 Executive Summary
2. Page 2 Daily System Pulse
3. Page 3 Monthly Stress and Pattern
4. Page 4 Country Exposure and Trade Reliance

## Step 8: Validate Page 1

Use these marts:

- `mart_dashboard_global_trade_overview`
- `mart_trade_month_coverage_status`
- `mart_executive_monthly_system_snapshot`
- `mart_chokepoint_monthly_stress_detail`

Check that Page 1 can reproduce:

- Latest Total Trade
- Reporting Completeness
- Stress Level
- Top Stressed Chokepoint
- Stressed Chokepoints
- trade trend over time
- reporting completeness trend
- stressed chokepoints score
- trade reporting table
- latest chokepoint movement table

Important:

- Page 1 is monthly and executive
- do not turn it into the main map page
- use display-safe stress fields for the main bar chart and executive table formatting

## Step 9: Validate Page 2

Use these marts:

- `mart_global_daily_market_signal`
- `mart_chokepoint_daily_signal`

Check that Page 2 can reproduce:

- Daily Coverage Status
- Observed Chokepoint Count
- Stress Chokepoint Count
- System Stress Level
- daily chokepoint movement/detail

Important:

- Page 2 is daily-only
- do not row-join monthly trade into Page 2
- partial daily coverage is expected when PortWatch freshness is uneven

## Step 10: Wire The Remaining Implemented Pages

After Pages 1 and 2 are stable, wire the code-complete pages:

### Page 3

Use:

- `mart_chokepoint_monthly_stress`
- `mart_global_monthly_system_stress_summary`

### Page 4

Use:

- `mart_reporter_partner_commodity_month_enriched`
- `mart_reporter_month_exposure_map`
- `mart_chokepoint_monthly_hotspot_map`

Important:

- the primary analytical map belongs on Page 4
- do not use non-map-safe exposure marts directly for maps
- do not build a default route-line world map yet

## Step 11: Leave Unfinished Scope For After Migration

Still unfinished after the last freeze:

- Page 5 Structural Vulnerability final semantic layer and Looker page
- Page 6 Historical Event / Trade Context final semantic layer and Looker page
- duplicate embedded Looker source cleanup
- final route-drill design, if still desired later

## Quick Recovery Checklist

If something fails, check in this order:

1. `make dbt-bigquery-debug`
2. confirm `.env` points to the new project
3. confirm `raw` tables exist in BigQuery
4. confirm `portwatch_daily` exists before testing Page 2
5. rerun the daily dbt selection
6. inspect `logs/dbt.log`
7. compare expected marts and fields against `DASHBOARD_FIELD_MAP.md`

## Minimum Success Definition

The migration is successful when:

- Terraform-managed infra exists in the new project
- raw landings exist in BigQuery
- full dbt build succeeds in `bigquery_dev`
- Page 1 matches the frozen executive screenshot closely
- Page 2 matches the frozen daily screenshot closely
- later pages can be connected from the semantic marts already in code
