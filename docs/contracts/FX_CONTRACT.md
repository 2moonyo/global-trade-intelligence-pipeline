# FX Contract

See also:

- [SHARED_WAREHOUSE_AND_SERVING_CONTRACT.md](./SHARED_WAREHOUSE_AND_SERVING_CONTRACT.md)

## Status

- Bronze ingest is scripted through `ingest/fred/fx_rates.py`
- Canonical monthly silver parquet is implemented through `ingest/fred/fx_silver.py`
- GCS publish is implemented in `warehouse/publish_fx_to_gcs.py`
- BigQuery raw landing is implemented in `warehouse/load_fx_to_bigquery.py`
- dbt staging and macro marts are BigQuery-facing

## Source Systems

- ECB EXR API

## Purpose

- Provide monthly FX context for macro feature modelling
- Convert ECB quote-per-EUR rates into a dbt-friendly USD context for macro marts

## Lifecycle By Phase

| Phase | Current implemented asset | Grain | Partitioning | Canonical path or table | Notes |
| --- | --- | --- | --- | --- | --- |
| Bronze standardized | FX daily standardized bronze CSVs | one row per `dt` x `series_id` | `dt=YYYY-MM-DD` | `data/bronze/ecb_fx_eu/dt=YYYY-MM-DD/part-*.csv` | Written via the FX extractor and shared bronze helpers. |
| Bronze batch | FX batch CSV snapshot | one row per `date` x `quote_ccy` x `base_ccy` | batch file | `data/bronze/ecb_fx_eu/Batch/ecb_fx_eu_<stamp>.csv` | Compatibility/export artifact, not the BigQuery raw contract. |
| Silver monthly | `ecb_fx_eu_monthly` | one row per `year_month` x `base_currency_code` x `quote_currency_code` | `year=YYYY/month=MM` | `data/silver/fx/ecb_fx_eu_monthly/year=YYYY/month=MM/ecb_fx_eu_monthly.parquet` | Canonical cloud-ready monthly asset. A consolidated snapshot is also written to `data/silver/fx/ecb_fx_eu_monthly.parquet`. |
| GCS publish | bronze and monthly silver | source asset grain | preserved from local shape | `gs://<bucket>/<prefix>/bronze/ecb_fx_eu/...`, `gs://<bucket>/<prefix>/silver/fx/ecb_fx_eu_monthly/...` | Checksum-aware publish with month filters. |
| BigQuery raw landing | `raw.ecb_fx_eu_monthly` | one row per `year_month` x currency pair | partition by `month_start_date`, cluster by `base_currency_code`, `quote_currency_code` | `raw.ecb_fx_eu_monthly` | Replaces touched month partitions by default and tracks load state. |
| dbt staging | `stg_fx_monthly` | one row per `year_month` x `fx_currency_code` | dbt-managed | analytics staging schema | Filters to EUR base rates and derives USD-per-currency values using the USD bridge row. |
| dbt marts | `mart_macro_monthly_features`, `mart_reporter_month_macro_features` | mart-specific | dbt-managed | analytics marts schema | FX is part of the macro context surface. |
| Semantic marts | macro/system pages that consume macro context | page-specific | dbt-managed | analytics marts schema | FX is contextual; no dedicated FX semantic mart exists yet. |

## Contract Decisions

- The current downstream path uses monthly silver parquet and BigQuery `raw.ecb_fx_eu_monthly`, not the older DuckDB batch CSV path.
- The `dt=` bronze partitions remain useful for reproducible extract history.
- Monthly FX is modelled at `year_month + base_currency_code + quote_currency_code` in raw and at `year_month + fx_currency_code` in dbt staging.
- `base_currency_code = EUR` rows are the current staged analytical surface.

## Monthly FX Computation

`ingest/fred/fx_silver.py`:

1. Reads standardized bronze FX rows.
2. Deduplicates daily observations by latest load timestamp.
3. Aggregates daily rates to monthly `fx_rate`.
4. Writes month partitions and a consolidated compatibility snapshot.
5. Calculates `fx_mom_change` by base/quote pair.

`stg_fx_monthly`:

1. Reads `raw.ecb_fx_eu_monthly`.
2. Keeps EUR-base quote currencies other than EUR.
3. Uses the EUR/USD row as the bridge.
4. Derives `fx_rate_to_usd`.

Justification:

- the ECB feed is quote-per-EUR-oriented
- downstream marts need a reporter-agnostic macro context benchmark in USD terms
- month partition replacement makes reruns safe without destructive bronze cleanup

## Minimum Required Fields

Silver and raw monthly:

- `year_month`
- `month_start_date`
- `year`
- `month`
- `base_currency_code`
- `quote_currency_code`
- `fx_rate`
- `trading_day_count`
- `source_row_count`
- `latest_load_ts`
- `fx_mom_change`
- `dataset_name`
- `source_name`

Monthly staged:

- `year_month`
- `month_start_date`
- `currency_view`
- `base_currency_code`
- `fx_currency_code`
- `fx_rate`
- `fx_rate_to_usd`
- `fx_mom_change`

## Current Operational Logging

Implemented logs and manifests exist for:

- extract: `logs/fx/fx_extract_manifest.jsonl`
- silver: `logs/fx/fx_silver_manifest.jsonl`
- GCS publish: `logs/fx/publish_fx_to_gcs.log`, `logs/fx/publish_fx_to_gcs_manifest.jsonl`
- BigQuery load: `logs/fx/load_fx_to_bigquery.log`, `logs/fx/load_fx_to_bigquery_manifest.jsonl`

## Known Gaps

- No dashboard page focuses on FX directly; it is currently contextual data only.
- FX is monthly in the serving layer. Daily FX is retained in bronze but is not currently modelled as a dbt daily mart.
