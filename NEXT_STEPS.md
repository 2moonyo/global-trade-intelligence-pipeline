# Next Steps

## Immediate Objective

Migrate the BigQuery + dbt + Looker Studio stack from the exhausted legacy GCP account into a new GCP account without losing:

- the implemented semantic marts
- the page-by-page dashboard design
- the daily/monthly separation rules
- the current executive and daily dashboard behavior

## Recommended Migration Order

### 1. Freeze and carry the repo

- Copy this repo exactly as-is.
- Keep the new handover markdown files at the repo root.
- Keep the screenshot freeze artifacts referenced in `PROJECT_STATE.md` and `DASHBOARD_STATUS.md`.
- Do not rely on the current git worktree being clean.
- Treat the repo as the source of truth, not the old GCP account.

### 2. Create the new GCP foundation

- Create a new GCP project.
- Update `infra/terraform/terraform.tfvars.json` with the new project id, bucket name, IAM members, and location.
- Keep bucket and BigQuery datasets in one consistent location.
- Strong recommendation: stay consistent with the prior deployed location unless you intentionally migrate location too.

Run:

```bash
make tfvars-init
make cloud-bootstrap
```

Then render env vars:

```bash
python infra/terraform/render_dotenv.py > .env
```

Or run dbt through the Makefile wrappers.

### 3. Recreate raw data landings in the new account

Recommended order:

1. PortWatch
2. Brent
3. Comtrade
4. FX
5. Events
6. World Bank energy

Recommended commands:

```bash
make portwatch-refresh-cloud
make brent-refresh-cloud
make comtrade-refresh-cloud
make fx-refresh-cloud
make events-refresh-cloud
```

World Bank energy currently uses scripts directly and may need a manual refresh path if not already wrapped in `Makefile`.

### 4. Rebuild dbt in BigQuery

Run a full build once raw landings are present:

```bash
make dbt-bigquery-build
```

If you want a phased validation sequence:

```bash
eval "$(python infra/terraform/render_dotenv.py --format export)"
UV_CACHE_DIR="$PWD/.uv-cache" uv run dbt build --profiles-dir . --target bigquery_dev --select models/staging
UV_CACHE_DIR="$PWD/.uv-cache" uv run dbt build --profiles-dir . --target bigquery_dev --select models/marts/dimensions
UV_CACHE_DIR="$PWD/.uv-cache" uv run dbt build --profiles-dir . --target bigquery_dev --select models/marts
UV_CACHE_DIR="$PWD/.uv-cache" uv run dbt build --profiles-dir . --target bigquery_dev --select models/marts/semantics
```

### 5. Validate the daily layer specifically

This is important because the daily layer had the last code fix before handoff.

Run:

```bash
eval "$(python infra/terraform/render_dotenv.py --format export)"
UV_CACHE_DIR="$PWD/.uv-cache" uv run dbt build --profiles-dir . --target bigquery_dev --select +mart_global_daily_market_signal +mart_chokepoint_daily_signal
```

Key checks:

- `stg_portwatch_daily` builds successfully
- `mart_chokepoint_daily_signal` builds successfully
- `mart_global_daily_market_signal` builds successfully
- `tests/stg_portwatch_daily_share_bounds.sql` passes

### 6. Reconnect Looker Studio data sources

Recreate or repoint the following data sources first:

- `mart_dashboard_global_trade_overview`
- `mart_trade_month_coverage_status`
- `mart_executive_monthly_system_snapshot`
- `mart_chokepoint_monthly_stress_detail`
- `mart_chokepoint_daily_signal`
- `mart_global_daily_market_signal`

Then add the Stage 5 sources:

- `mart_reporter_partner_commodity_month_enriched`
- `mart_reporter_month_exposure_map`
- `mart_chokepoint_monthly_hotspot_map`

Restore and validate these pages in this order:

1. Page 1 Executive Summary
2. Page 2 Daily System Pulse
3. Page 3 Monthly Stress and Pattern
4. Page 4 Country Exposure and Trade Reliance

### 7. Rebuild dashboard pages in order

Recommended order:

1. Page 1 Executive Summary
2. Page 2 Daily System Pulse
3. Page 3 Monthly Stress and Pattern
4. Page 4 Country Exposure and Trade Reliance
5. Page 5 Structural Vulnerability
6. Page 6 Historical Event / Trade Context

## What Is Already Done In Code

- Page 1 semantic support is complete.
- Page 2 semantic support is complete.
- Page 3 semantic support is complete.
- Page 4 semantic support is complete in code.
- The Page 2 daily division bug was fixed in code.
- Tests were added across daily, monthly, executive, and exposure models.

## What Still Needs To Be Done

### Highest priority

- migrate to the new GCP project
- reload raw datasets
- rebuild dbt in BigQuery
- reconnect Looker data sources
- verify Page 1 and Page 2 match the frozen screenshots

### Next priority

- wire Page 3 into Looker Studio
- wire Page 4 into Looker Studio
- clean up duplicate embedded Looker sources

### After that

- build Page 5 semantic layer and Looker page
- build Page 6 semantic layer and Looker page

## Suggested Validation Checklist

### Data platform

- bucket exists
- `raw` dataset exists
- `analytics` dataset exists
- ADC auth works
- dbt BigQuery target connects

### Raw tables

- `raw.portwatch_daily`
- `raw.portwatch_monthly`
- `raw.brent_daily`
- `raw.brent_monthly`
- `raw.comtrade_fact`
- `raw.dim_trade_routes`
- `raw.ecb_fx_eu_monthly`
- `raw.energy_vulnerability`
- `raw.bridge_event_month_chokepoint_core`
- `raw.bridge_event_month_maritime_region`

### dbt semantics

- all models in `models/marts/semantics` build
- semantic tests pass
- no skipped models caused by missing upstream tables

### Looker

- Page 1 cards and charts render
- Page 2 cards and charts render
- filters work on `month_start_date` and `date_day`
- no map uses the wrong mart grain

## Migration Cautions

- Do not carry over old API key values blindly; re-enter secrets intentionally.
- Do not let BigQuery and GCS end up in different locations.
- Do not rebuild Page 4 using the old non-map-safe exposure marts directly in Looker.
- Do not use `mart_reporter_month_macro_features` as a one-row-per-country-month source.
- Do not merge daily and monthly data into a single Looker source.
