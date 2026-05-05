# World Bank Energy Contract

See also:

- [SHARED_WAREHOUSE_AND_SERVING_CONTRACT.md](./SHARED_WAREHOUSE_AND_SERVING_CONTRACT.md)

## Status

- Bronze ingest is scripted through `ingest/world_bank/worldbank_energy.py`
- Canonical annual silver parquet is implemented through `ingest/world_bank/worldbank_energy_silver.py`
- GCS publish is implemented in `warehouse/publish_worldbank_energy_to_gcs.py`
- BigQuery raw landing is implemented in `warehouse/load_worldbank_energy_to_bigquery.py`
- dbt staging, energy marts, and the Page 5 structural vulnerability mart consume this dataset

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
- Enrich reporter-month macro context and structural vulnerability storytelling

## Lifecycle By Phase

| Phase | Current implemented asset | Grain | Partitioning | Canonical path or table | Notes |
| --- | --- | --- | --- | --- | --- |
| Bronze partitioned | normalized JSONL rows | one row per `dt` x `country` x `indicator_alias` | `dt=YYYY-01-01` | `data/bronze/worldbank_energy/dt=YYYY-01-01/part-<stamp>.jsonl` | Written by the extractor. |
| Bronze batch | batch CSV snapshot | one row per `country` x `year` x `indicator_alias` | batch file | `data/bronze/worldbank_energy/Batch/worldbank_energy_<stamp>.csv` | Compatibility/export artifact. |
| Silver annual | `energy_vulnerability` | one row per `country_iso3` x `year` x `indicator_code` | `year=YYYY` | `data/silver/worldbank_energy/energy_vulnerability/year=YYYY/energy_vulnerability.parquet` | Canonical cloud-ready annual long-format asset. |
| Bronze operational metadata | run manifest, rolling log, and API metadata bundle | one row per extract run / one rolling log / one metadata JSON per run | append-only JSONL / log file / JSON | `logs/worldbank_energy/worldbank_energy_extract_manifest.jsonl`, `logs/worldbank_energy/worldbank_energy_extract.log`, `data/metadata/worldbank_energy/worldbank_energy_api_metadata_<run_id>.json` | Captures selected countries, indicators, years, request templates, and errors. |
| Silver operational metadata | silver manifest and rolling log | one row per silver build / one rolling log | append-only JSONL / log file | `logs/worldbank_energy/worldbank_energy_silver_manifest.jsonl`, `logs/worldbank_energy/worldbank_energy_silver.log` | Captures bronze files read, years written, dedupe counts, null-value counts, and partition outputs. |
| Bronze preview | optional wide preview CSV | one row per `country_iso3` x `year` | batch file | `data/bronze/worldbank_energy/Preview/worldbank_energy_wide_<stamp>.csv` | Convenience output, not downstream contract. |
| GCS publish | metadata, bronze, and annual silver | source asset grain | preserved from local shape | `gs://<bucket>/<prefix>/metadata/worldbank_energy/...`, `gs://<bucket>/<prefix>/bronze/worldbank_energy/...`, `gs://<bucket>/<prefix>/silver/worldbank_energy/energy_vulnerability/...` | Checksum-aware publish with year filters. |
| BigQuery raw landing | `raw.energy_vulnerability` | one row per `country_iso3` x `year` x `indicator_code` | partition by `month_start_date`, cluster by `indicator_code`, `country_iso3` | `raw.energy_vulnerability` | Replaces touched years by default and tracks load state. |
| dbt staging | `stg_energy_vulnerability` | one row per `reporter_iso3` x `year` x `indicator_code` | dbt-managed | analytics staging schema | Standardizes and deduplicates long-format annual indicators. |
| dbt marts | `mart_reporter_energy_vulnerability`, `mart_reporter_month_macro_features`, `mart_reporter_structural_vulnerability` | mart-specific | dbt-managed | analytics marts schema | Energy is conformed and then broadcast to reporter-month rows where needed. |
| Semantic mart | `mart_reporter_structural_vulnerability` | one row per reporter x month | dbt-managed | analytics marts schema | Page 5 structural vulnerability surface. |

## Contract Decisions

- Annual energy indicators are intentionally kept at country-year grain in silver and raw.
- The current BigQuery path reads annual silver parquet, not batch CSV.
- Annual values are broadcast across months in dbt by matching `reporter_iso3 + year` where a reporter-month mart requires monthly grain.
- Special World Bank aggregate/geography rows are allowed in raw/staging but may be filtered or flagged by country-dimension eligibility in downstream map or reporter marts.

## Minimum Required Fields

Silver and raw essentials:

- `dt`
- `month_start_date`
- `year`
- `dataset`
- `source`
- `ingest_ts`
- `indicator_alias`
- `indicator_code`
- `indicator_id`
- `indicator_name`
- `metric_name`
- `unit_hint`
- `country_name`
- `country_id`
- `country_iso3`
- `value`
- `wb_unit`
- `obs_status`
- `decimal_places`
- `grain_key`

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

## Downstream Use

- `mart_reporter_energy_vulnerability` exposes the conformed long-format annual indicators.
- `mart_reporter_month_macro_features` broadcasts annual indicators into reporter-month macro context.
- `mart_reporter_structural_vulnerability` pivots selected indicators into Page 5 fields such as `energy_import_pct`, `renewable_share_pct`, and `fossil_share_pct`.

## Known Gaps

- Not all energy rows conform cleanly to the country dimension because some codes are aggregates or special entities.
- The extractor can run in narrower refresh modes based on database countries and existing years; a full historical World Bank backfill must be requested intentionally.
- Energy data remains annual; monthly presentation is a controlled broadcast, not a true monthly measurement.
