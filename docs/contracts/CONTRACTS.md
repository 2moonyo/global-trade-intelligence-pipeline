# Contracts Index

This directory separates the warehouse contracts into one shared platform contract and one contract per dataset.

## Contract Maintenance Rule

Update these contract documents whenever a major repository change affects any of the following:

- active dbt targets or default target selection
- BigQuery raw or analytics dataset naming
- canonical local silver, GCS publish, or BigQuery load paths
- dbt sources, staging models, marts, or semantic/dashboard marts
- the primary serving layer exposed to BI users
- dataset refresh and replacement behavior

Current verified environment:

- dbt profile: `capstone_monthly`
- active dbt target: `bigquery_dev`
- raw landing dataset: `raw` by default, override with `GCP_BIGQUERY_RAW_DATASET`
- analytics dataset base: `analytics` by default, override with `DBT_BIGQUERY_DATASET` or `GCP_BIGQUERY_ANALYTICS_DATASET`
- dbt model schemas from `dbt_project.yml`:
  - staging models -> `<analytics_dataset>_staging`
  - marts models -> `<analytics_dataset>_marts`
- serving path: BigQuery dbt marts and semantic marts for Looker Studio or equivalent BI
- local files remain the operational bronze/silver source of truth before GCS publish

## Shared Contracts

- [SHARED_WAREHOUSE_AND_SERVING_CONTRACT.md](./SHARED_WAREHOUSE_AND_SERVING_CONTRACT.md)
  - VM-first architecture
  - GCS and BigQuery raw landing conventions
  - dbt target and schema conventions
  - cross-source canonical rules
  - dashboard/semantic mart serving contract
  - quality and migration expectations

## Dataset Contracts

- [PORTWATCH_CONTRACT.md](./PORTWATCH_CONTRACT.md)
- [COMTRADE_CONTRACT.md](./COMTRADE_CONTRACT.md)
- [BRENT_CONTRACT.md](./BRENT_CONTRACT.md)
- [FX_CONTRACT.md](./FX_CONTRACT.md)
- [WORLDBANK_ENERGY_CONTRACT.md](./WORLDBANK_ENERGY_CONTRACT.md)
- [EVENTS_CONTRACT.md](./EVENTS_CONTRACT.md)

## Current Maturity Snapshot

| Dataset | Local silver maturity | GCS/BigQuery maturity | Current primary serving path |
| --- | --- | --- | --- |
| Comtrade | high, but operationally complex | implemented for fact, core dimensions, routes, and audit/state tables | BigQuery dbt marts and semantic marts |
| PortWatch | high | implemented for daily and monthly raw tables | BigQuery dbt semantic marts |
| Brent | high for daily/monthly silver | implemented for daily and monthly raw tables | BigQuery dbt macro and signal marts |
| FX | high for monthly silver | implemented for monthly raw table | BigQuery dbt macro marts |
| World Bank energy | high for annual silver | implemented for annual raw table | BigQuery dbt structural vulnerability mart |
| Events | high for curated generated silver | implemented for event dimension and bridge tables | BigQuery dbt event dimensions, bridges, and semantic marts |

## Reading Order

If you want the full picture, read in this order:

1. [SHARED_WAREHOUSE_AND_SERVING_CONTRACT.md](./SHARED_WAREHOUSE_AND_SERVING_CONTRACT.md)
2. the dataset contract you are working on
3. any downstream dataset that consumes it

Recommended dependency order:

1. Comtrade
2. PortWatch
3. Brent
4. FX
5. World Bank energy
6. Events
