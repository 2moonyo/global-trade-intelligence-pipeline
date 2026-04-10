# Architecture

## Architecture Summary

The target serving architecture is:

1. Source APIs / files
2. Local bronze extraction outputs in `data/bronze/*`
3. Local silver contract outputs in `data/silver/*`
4. GCS publish layer
5. BigQuery `raw` landing tables
6. dbt staging views in `analytics_staging`
7. dbt marts and dimensions in `analytics_marts`
8. Looker Studio semantic consumption

This repo still contains DuckDB and Streamlit assets, but they are legacy local-run artifacts and are not part of the target dashboard build.

## Contract Sources Of Truth

Use these files as the authoritative contract references during migration:

- `models/sources/silver_sources.yml` for required BigQuery raw tables
- `models/staging/schema.yml` for staging model grains and column expectations
- `models/marts/schema.yml` for warehouse facts and non-semantic marts
- `models/marts/semantics/schema.yml` for Looker-facing semantic mart contracts
- the root handover markdown files for the frozen dashboard and migration sequence

## Core Design Principle: Daily vs Monthly Separation

### Daily layer

Purpose:

- operational signal detection
- current system status
- recent deviations

Current daily sources:

- PortWatch daily
- Brent daily

Current daily semantic marts:

- `mart_chokepoint_daily_signal`
- `mart_global_daily_market_signal`

### Monthly layer

Purpose:

- structural explanation
- coverage and missingness framing
- historical comparison
- exposure and country storytelling

Current monthly semantic marts:

- `mart_dashboard_global_trade_overview`
- `mart_trade_month_coverage_status`
- `mart_executive_monthly_system_snapshot`
- `mart_chokepoint_monthly_stress`
- `mart_chokepoint_monthly_stress_detail`
- `mart_global_monthly_system_stress_summary`
- `mart_reporter_partner_commodity_month_enriched`
- `mart_reporter_month_exposure_map`
- `mart_chokepoint_monthly_hotspot_map`

## Current BigQuery Contract Shape

### Raw dataset

Current raw landing contract is a curated raw layer, not a strict bronze mirror.

Active raw tables declared in `models/sources/silver_sources.yml`:

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

### Staging contracts

Important staging models and their grains:

| Model | Grain | Purpose |
| --- | --- | --- |
| `stg_portwatch_daily` | `date_day + chokepoint_id` | scaffolded daily PortWatch signals |
| `stg_portwatch_stress_metrics` | `year_month + chokepoint_id` | monthly PortWatch stress baseline layer |
| `stg_brent_daily` | `date_day + benchmark_code` | deduped daily Brent |
| `stg_brent_monthly` | `year_month + benchmark_code` | monthly Brent |
| `stg_comtrade_trade_base` | canonical trade row | cleaned Comtrade fact landing |
| `stg_comtrade_fact` | canonical trade row | standardized trade view |
| `stg_dim_country` | `iso3` | country dimension |
| `stg_dim_commodity` | `cmd_code` | commodity dimension |
| `stg_dim_time` | `period` / month | conformed monthly time dimension |
| `stg_dim_chokepoint` | `chokepoint_id` | chokepoint dimension with geography |
| `stg_dim_country_ports` | `iso3 + port` | representative ports per country |
| `stg_dim_trade_route_geography` | route-level | route path geometry and route metadata |
| `stg_fx_monthly` | `year_month + currency_view + fx_currency_code` | monthly FX context |
| `stg_energy_vulnerability` | `reporter_iso3 + year + indicator_code` | annual energy context |
| `stg_chokepoint_bridge` | event-month-chokepoint | event overlay bridge |

## Core Fact and Non-Semantic Mart Layer

These models are reused by the semantic layer:

| Model | Grain | Notes |
| --- | --- | --- |
| `fct_reporter_partner_commodity_month` | reporter + partner + commodity + month + flow | canonical monthly trade fact |
| `fct_reporter_partner_commodity_route_month` | canonical trade grain with route enrichment | route fields, chokepoints, confidence, route group |
| `fct_reporter_partner_commodity_hub_month` | reporter + partner + partner2 + commodity + month + flow | hub/transshipment expansion |
| `mart_reporter_month_trade_summary` | reporter + month | reporter totals |
| `mart_reporter_commodity_month_trade_summary` | reporter + commodity + month | commodity totals |
| `mart_reporter_month_chokepoint_exposure` | reporter + month + chokepoint | monthly exposure backbone |
| `mart_trade_exposure` | reporter + month + chokepoint + route_confidence_score | confidence-aware exposure |
| `mart_reporter_energy_vulnerability` | reporter + year + indicator | future Page 5 support |
| `mart_event_impact` | event grain | future Page 6 support |

## Semantic Layer Contracts

### Page 1 semantics

| Mart | Grain | Safe filters | Purpose |
| --- | --- | --- | --- |
| `mart_dashboard_global_trade_overview` | `reporter_country_code + month_start_date` | reporter, month | executive trade overview and missingness |
| `mart_trade_month_coverage_status` | `month_start_date` | month | trade completeness trend and scorecards |
| `mart_executive_monthly_system_snapshot` | `month_start_date` | month | executive monthly system pulse |
| `mart_chokepoint_monthly_stress_detail` | `month_start_date + chokepoint_id` | month, chokepoint | ranked chokepoint detail table |

### Page 2 semantics

| Mart | Grain | Safe filters | Purpose |
| --- | --- | --- | --- |
| `mart_chokepoint_daily_signal` | `date_day + chokepoint_id` | date, chokepoint | daily chokepoint operations |
| `mart_global_daily_market_signal` | `date_day` | date | daily scorecards and Brent overlay |

### Page 3 semantics

| Mart | Grain | Safe filters | Purpose |
| --- | --- | --- | --- |
| `mart_chokepoint_monthly_stress` | `month_start_date + chokepoint_id` | month, chokepoint | monthly stress analysis |
| `mart_global_monthly_system_stress_summary` | `month_start_date` | month | monthly system summary |

### Page 4 semantics

| Mart | Grain | Safe filters | Purpose |
| --- | --- | --- | --- |
| `mart_reporter_partner_commodity_month_enriched` | `month_start_date + reporter_iso3 + partner_iso3 + cmd_code + chokepoint_id` | reporter, partner, commodity, chokepoint, route group, month | detailed exposure table |
| `mart_reporter_month_exposure_map` | `month_start_date + reporter_iso3` | reporter, month | country map |
| `mart_chokepoint_monthly_hotspot_map` | `month_start_date + chokepoint_id` | chokepoint, month | chokepoint point map |

## Dataset Contracts And Semantic Filtering Rules

This section is the practical contract for migration and future work.

### `mart_dashboard_global_trade_overview`

- Upstream: `mart_reporter_month_trade_summary`
- Filtered by: reporter, month
- Not safe for: country maps unless aggregated first
- Special role: must remain the Page 1 trade anchor

### `mart_trade_month_coverage_status`

- Upstream: `mart_dashboard_global_trade_overview`
- Filtered by: month only
- Purpose: scorecards, trend lines, completeness lag
- Not safe for: reporter tables

### `mart_executive_monthly_system_snapshot`

- Upstream: `mart_global_monthly_system_stress_summary`, `mart_chokepoint_monthly_stress`
- Filtered by: month only
- Purpose: executive monthly system summary
- Not safe for: detailed chokepoint analysis

### `mart_chokepoint_monthly_stress_detail`

- Upstream: `mart_chokepoint_monthly_stress`
- Filtered by: month, chokepoint
- Purpose: Page 1 ranked table
- Display-safe fields available: capped deviation score, 0-100 index, severity band

### `mart_chokepoint_daily_signal`

- Upstream: `stg_portwatch_daily`
- Filtered by: date, chokepoint
- Purpose: Page 2 daily signal table and charts
- Not safe for: monthly trade joins at row grain

### `mart_global_daily_market_signal`

- Upstream: `mart_chokepoint_daily_signal`, `stg_brent_daily`
- Filtered by: date only
- Purpose: Page 2 daily scorecards
- Not safe for: country, partner, or commodity analysis

### `mart_chokepoint_monthly_stress`

- Upstream: `stg_portwatch_stress_metrics`, `stg_chokepoint_bridge`
- Filtered by: month, chokepoint
- Purpose: Page 3 monthly stress

### `mart_global_monthly_system_stress_summary`

- Upstream: `mart_chokepoint_monthly_stress`
- Filtered by: month only
- Purpose: Page 3 scorecards

### `mart_reporter_partner_commodity_month_enriched`

- Upstream:
  - `fct_reporter_partner_commodity_route_month`
  - `mart_reporter_month_trade_summary`
  - `mart_reporter_month_chokepoint_exposure`
  - `mart_chokepoint_monthly_stress_detail`
  - `dim_country`
  - `dim_commodity`
  - `dim_chokepoint`
- Filtered by:
  - reporter
  - partner
  - commodity
  - chokepoint
  - route group
  - month
- Purpose: Page 4 analytical table
- Not safe for: country map or chokepoint point map directly

### `mart_reporter_month_exposure_map`

- Upstream:
  - `mart_reporter_month_chokepoint_exposure`
  - `mart_trade_exposure`
  - `dim_country`
- Filtered by:
  - reporter
  - month
- Geography role:
  - country map only
- Not safe for:
  - chokepoint point maps
  - route-line maps

### `mart_chokepoint_monthly_hotspot_map`

- Upstream:
  - `mart_chokepoint_monthly_stress_detail`
  - `mart_reporter_month_chokepoint_exposure`
  - `mart_trade_exposure`
  - `dim_chokepoint`
- Filtered by:
  - chokepoint
  - month
- Geography role:
  - chokepoint point map only
- Not safe for:
  - country choropleths
  - route-line maps

## Geospatial Architecture

### Country geography

- Use `reporter_iso3` and `reporter_country_name` in Looker Studio.
- Keep maps at `month_start_date + reporter_iso3`.
- Do not use multi-row exposure marts directly for country choropleths.

### Chokepoint geography

- Use `chokepoint_id`, `chokepoint_name`, `latitude`, `longitude`.
- Keep maps at `month_start_date + chokepoint_id`.

### Route geometry

- Upstream route geometry exists in `stg_dim_trade_route_geography`.
- Default route-line map remains deferred.
- Do not present modeled route lines as observed vessel tracks.

## Testing Strategy In Place

Current tests added during this work include:

- unique grain tests
- share-bounds tests
- coverage-bounds tests
- previous-month consistency tests
- route-context stability tests
- daily lookback requirement tests
- stress display bounds tests

These tests live in `tests/` and are intended to be rerun after migration.

## Migration-Sensitive Architecture Notes

- `profiles.yml` defaults to `duckdb_dev`; BigQuery work depends on env vars.
- Previous cloud configuration used `us-central1`.
- The shared `safe_divide` macro was fixed locally and must be included in the migrated repo.
- The semantic layer is in source control; the old Looker data sources are not.
- Only Page 1 and Page 2 have screenshot-confirmed Looker freeze artifacts.
