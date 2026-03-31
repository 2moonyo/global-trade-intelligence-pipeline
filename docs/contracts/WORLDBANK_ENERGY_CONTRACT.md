# World Bank Energy Contract

See also:

- [SHARED_WAREHOUSE_AND_SERVING_CONTRACT.md](./SHARED_WAREHOUSE_AND_SERVING_CONTRACT.md)

## Status

- Bronze ingest is scripted and operationally mature
- Local DuckDB, dbt staging, marts, and dashboard use are implemented
- No cloud landing path is provisioned yet

## Source Systems

- World Bank Indicators API v2
- Indicator aliases currently used:
  - `renew`
  - `fossil`
  - `imports`
  - `oil`
  - `gas`
  - `coal`

## Purpose

- Provide structural annual energy-vulnerability indicators by country
- Enrich reporter-month macro context and dashboard narrative

## Lifecycle By Phase

| Phase | Current implemented asset | Grain | Partitioning | Canonical path or table | Notes |
| --- | --- | --- | --- | --- | --- |
| Bronze local partitioned | normalized JSONL rows | one row per `dt` x `country` x `indicator_alias` | `dt=YYYY-01-01` | `data/bronze/worldbank_energy/dt=YYYY-01-01/part-<stamp>.jsonl` | Written by `ingest/world_bank/worldbank_energy.py`. |
| Bronze local batch | batch CSV snapshot | one row per `country` x `year` x `indicator_alias` | batch file | `data/bronze/worldbank_energy/Batch/worldbank_energy_<stamp>.csv` | Current source for DuckDB bootstrap. |
| Silver local partitioned | typed annual long-format parquet | one row per `country_iso3` x `year` x `indicator_code` | `year=YYYY` | `data/silver/worldbank_energy/energy_vulnerability/year=YYYY/energy_vulnerability.parquet` | Written by `ingest/world_bank/worldbank_energy_silver.py`; deduplicates to latest `ingest_ts` per annual grain and sorts rows by indicator then country. |
| Bronze operational metadata | run manifest, rolling log, and API metadata bundle | one row per extract run / one rolling log / one metadata JSON per run | append-only JSONL / log file / JSON | `logs/worldbank_energy/worldbank_energy_extract_manifest.jsonl`, `logs/worldbank_energy/worldbank_energy_extract.log`, `data/metadata/worldbank_energy/worldbank_energy_api_metadata_<run_id>.json` | Captures run id, timing, errors, selected countries, selected energy types, year mode, and request templates used to call the API. |
| Silver operational metadata | silver manifest and rolling log | one row per silver build / one rolling log | append-only JSONL / log file | `logs/worldbank_energy/worldbank_energy_silver_manifest.jsonl`, `logs/worldbank_energy/worldbank_energy_silver.log` | Captures bronze files read, years written, dedupe counts, null-value counts, and partition outputs. |
| Bronze preview | optional wide preview CSV | one row per `country_iso3` x `year` | batch file | `data/bronze/worldbank_energy/Preview/worldbank_energy_wide_<stamp>.csv` | Convenience output, not downstream contract. |
| DuckDB raw landing | `raw.energy_vulnerability` | one row per `country_iso3` x `year` x `indicator_alias` x `ingest_ts` | DuckDB table | `raw.energy_vulnerability` | Loaded from annual silver parquet partitions. |
| dbt staging | `stg_energy_vulnerability` | one row per `reporter_iso3` x `year` x `indicator_code` | dbt-managed | analytics schemas | Standardizes and deduplicates long-format annual indicators. |
| dbt marts | `mart_reporter_energy_vulnerability`, `mart_reporter_month_macro_features` | mart-specific | dbt-managed | analytics schemas | Energy is conformed and then broadcast to reporter-month rows. |
| Dashboard | Streamlit page 5 | country-year and reporter-month context | page-level filters | `app/pages/5_Energy_Vulnerability_Context.py` | This is the current user-facing energy consumer. |

## Contract Decisions

- Annual energy indicators are intentionally kept at country-year grain.
- The current local bootstrap reads batch CSV files, while partitioned JSONL provides the more future-proof bronze interface.
- Annual energy indicators are deliberately broadcast across months by matching on `reporter_iso3 + year` in `mart_reporter_month_macro_features`.

## Minimum Required Fields

Raw and staged essentials:

- `dt`
- `year`
- `indicator_alias`
- `indicator_id`
- `indicator_name`
- `metric_name`
- `country_name`
- `country_iso3`
- `value`
- `ingest_ts`

Staged and mart essentials:

- `reporter_iso3`
- `year`
- `period`
- `year_month`
- `indicator_code`
- `indicator_value`
- `unit`
- `country_match_status`

## Current Deduplication Rule

`stg_energy_vulnerability` keeps the latest `ingest_ts` for each:

- `reporter_iso3`
- normalized country name
- `year`
- `indicator_code`

Justification:

- the World Bank API is annual and slowly changing
- latest-ingest wins is sufficient for current analytical needs
- retaining annual grain avoids false precision when values do not genuinely vary by month

## Dashboard Use

The Energy Vulnerability Context page:

- ranks reporters on a selected indicator from `mart_reporter_energy_vulnerability`
- trends the selected indicator over time
- joins energy trade scale from `mart_reporter_commodity_month_trade_summary` using HS `2709` and `2710`
- joins latest reporter exposure from `mart_trade_exposure`

## Known Gaps

- Not all energy rows conform cleanly to the country dimension because some codes are aggregates or special entities.
- The bronze partitioned JSONL contract is more expressive than the current raw bootstrap, which still reads batch CSV.
- The current extractor now defaults to `db-countries + existing-db years`, which is convenient for warehouse refreshes but narrower than a full historical World Bank backfill.
