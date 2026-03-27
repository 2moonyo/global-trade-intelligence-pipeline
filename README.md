# DuckDB + dbt Analytics Warehouse

This repository contains the implemented DuckDB warehouse and dbt project for the capstone analytics stack. The current build is larger than the original bootstrap: it includes core trade facts, route and hub enrichments, macro and energy marts, event impact modelling, canonical-grain provenance, a dbt-generated time dimension, and a generic event location layer.

## Warehouse shape

The live warehouse is organized as:

1. filesystem data assets in `data/bronze/*` and `data/silver/*`
2. a `raw` landing schema loaded by `warehouse/bootstrap_silver_to_duckdb.sql`
3. dbt staging models in `analytics_staging` and `analytics_analytics_staging`
4. dbt marts in `analytics_marts` and `analytics_analytics_marts`

This is not a strict in-database medallion layout. In practice:

- trade, routing, PortWatch, Brent, and event raw tables are loaded mostly from curated silver outputs
- ECB FX and World Bank energy raw tables are loaded from bronze CSV batches
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

## Build and refresh

### 1. Load raw tables into DuckDB

Run from project root:

```bash
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
uv run dbt run --profiles-dir . --select staging
uv run dbt run --profiles-dir . --select marts
uv run dbt test --profiles-dir .
```

## Useful dbt vars

`stg_dim_time` supports buffered calendar generation:

```bash
uv run dbt run --profiles-dir . --select stg_dim_time --vars '{"dim_time_lag_months": 12, "dim_time_lead_months": 12}'
```

Default behavior already uses a 12-month lag and 12-month lead.

## Current implementation notes

- `canonical_grain_key` is carried through staged and fact trade models to support downstream lineage joins.
- `fct_reporter_partner_commodity_month_provenance` is the canonical-grain provenance table for auditability and future ingest-log integration.
- PortWatch exposure and event impact both use chokepoint stress signals, but they are not the same metric family: exposure marts use upstream stress indices, while event impact uses dbt-computed z-scores from `stg_chokepoint_stress_zscore`.
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
