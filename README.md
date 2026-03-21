# DuckDB + dbt Warehouse Bootstrap

This repository now includes a DuckDB warehouse bootstrap and dbt models for your silver-layer analytics.

## What was added

- DuckDB loader script: `warehouse/load_silver_to_duckdb.py`
- Warehouse bootstrap SQL: `warehouse/bootstrap_silver_to_duckdb.sql`
- dbt project config: `dbt_project.yml` and `profiles.yml`
- Source definitions: `models/sources/silver_sources.yml`
- Staging models:
	- `stg_comtrade_fact`
	- `stg_dim_country`
	- `stg_dim_time`
	- `stg_dim_commodity`
	- `stg_dim_trade_flow`
	- `stg_route_applicability`
	- `stg_chokepoint_bridge`
- Marts:
	- `mart_reporter_month_trade_summary`
	- `mart_reporter_commodity_month_trade_summary`
	- `mart_reporter_month_chokepoint_exposure`

## Step 1: Load silver tables into DuckDB

Run from project root:

```bash
uv run python warehouse/load_silver_to_duckdb.py
```

This creates/updates `warehouse/analytics.duckdb` and populates `raw.*` tables.

## Step 2: Build dbt staging models

Install/sync dependencies if needed:

```bash
uv sync
```

Run dbt:

```bash
uv run dbt debug --profiles-dir .
uv run dbt run --profiles-dir . --select staging
```

## Step 3: Build starter marts

```bash
uv run dbt run --profiles-dir . --select marts
```

Or run all models:

```bash
uv run dbt build --profiles-dir .
```

## Quick query examples

```sql
-- top 20 reporter-months by total value
select *
from marts.mart_reporter_month_trade_summary
order by total_trade_value_usd desc
limit 20;

-- top 20 reporter-commodity-month rows
select *
from marts.mart_reporter_commodity_month_trade_summary
order by total_trade_value_usd desc
limit 20;

-- highest chokepoint exposure ratios
select *
from marts.mart_reporter_month_chokepoint_exposure
order by chokepoint_trade_exposure_ratio desc nulls last
limit 20;
```
