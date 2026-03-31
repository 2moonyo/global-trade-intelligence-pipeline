# Contracts Index

This directory now separates the warehouse contracts into one shared platform contract and one contract per dataset.

## Shared Contracts

- [SHARED_WAREHOUSE_AND_SERVING_CONTRACT.md](./SHARED_WAREHOUSE_AND_SERVING_CONTRACT.md)
  - current local architecture
  - current cloud target architecture
  - cross-source canonical rules
  - Streamlit serving contract
  - shared dbt marts and model justification
  - quality and migration expectations

## Dataset Contracts

- [PORTWATCH_CONTRACT.md](./PORTWATCH_CONTRACT.md)
- [COMTRADE_CONTRACT.md](./COMTRADE_CONTRACT.md)
- [BRENT_CONTRACT.md](./BRENT_CONTRACT.md)
- [FX_CONTRACT.md](./FX_CONTRACT.md)
- [WORLDBANK_ENERGY_CONTRACT.md](./WORLDBANK_ENERGY_CONTRACT.md)
- [EVENTS_CONTRACT.md](./EVENTS_CONTRACT.md)

## Current Maturity Snapshot

| Dataset | Local contract maturity | Cloud maturity | Current primary serving path |
| --- | --- | --- | --- |
| PortWatch | high | high relative to the repo; first working vertical slice | DuckDB + Streamlit locally, with GCS and BigQuery scaffolded |
| Comtrade | high locally | low | DuckDB + Streamlit |
| Brent | medium | low | DuckDB via downstream marts |
| FX | medium | low | DuckDB via downstream marts |
| World Bank energy | medium-high | low | DuckDB + Streamlit |
| Events | medium locally, but manually curated | low | DuckDB + Streamlit |

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
[](![]())