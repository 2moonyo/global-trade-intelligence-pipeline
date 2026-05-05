# Brent Contract

See also:

- [SHARED_WAREHOUSE_AND_SERVING_CONTRACT.md](./SHARED_WAREHOUSE_AND_SERVING_CONTRACT.md)

## Status

- Bronze ingest is scripted with per-run logs and manifest output
- Curated silver daily and monthly parquet partitions are implemented
- GCS publish is implemented in `warehouse/publish_brent_to_gcs.py`
- BigQuery raw landing is implemented for both daily and monthly Brent tables
- dbt staging, macro marts, and daily market signal semantics consume Brent data

## Source Systems

- FRED API
- Series currently used in code:
  - `DCOILBRENTEU`
  - `DCOILWTICO`
  - `POILBREUSDM`

## Purpose

- Provide oil price context for macro features, chokepoint exposure overlays, and daily market signal reporting

## Lifecycle By Phase

| Phase | Current implemented asset | Grain | Partitioning | Canonical path or table | Notes |
| --- | --- | --- | --- | --- | --- |
| Bronze local daily | Brent/WTI daily CSV extracts | one row per `date` x benchmark/region | `year=YYYY/month=MM/day=DD` | `data/bronze/brent/year=YYYY/month=MM/day=DD/brent_prices_YYYYMMDD.csv` | Written by `ingest/fred/brent_crude.py`. |
| Bronze local batch | Brent batch snapshot | one row per `date` x benchmark/region | batch file | `data/bronze/brent/Batch/brent_crude_<stamp>.csv` | Compatibility/export artifact. |
| Silver daily | `brent_daily` | one row per trading date x benchmark | `year=YYYY/month=MM` | `data/silver/brent/brent_daily/year=YYYY/month=MM/brent_daily.parquet` | Built by `ingest/fred/brent_silver.py`. A consolidated compatibility snapshot is also written to `data/silver/brent/brent_daily.parquet`. |
| Silver monthly | `brent_monthly` | one row per `year_month` x benchmark | `year=YYYY/month=MM` | `data/silver/brent/brent_monthly/year=YYYY/month=MM/brent_monthly.parquet` | Built by `ingest/fred/brent_silver.py`. A consolidated compatibility snapshot is also written to `data/silver/brent/brent_monthly.parquet`. |
| GCS publish | Brent daily and monthly parquet partitions | table-specific | `year=YYYY/month=MM` | `gs://<bucket>/<prefix>/silver/brent/brent_daily/...`, `gs://<bucket>/<prefix>/silver/brent/brent_monthly/...` | Checksum-aware publish with month filters. |
| BigQuery raw landing | `raw.brent_daily`, `raw.brent_monthly` | table-specific | `brent_daily` partitioned by `trade_date`; `brent_monthly` partitioned by `month_start_date`; both clustered by `benchmark_code` | `raw.brent_daily`, `raw.brent_monthly` | Replaces touched month partitions by default and tracks load state. |
| dbt staging | `stg_brent_daily`, `stg_brent_monthly` | table-specific | dbt-managed | analytics staging schema | Deduplicates daily rows and standardizes monthly series. |
| dbt marts | `mart_macro_monthly_features`, `mart_reporter_month_macro_features`, `mart_reporter_month_chokepoint_exposure_with_brent` | mart-specific | dbt-managed | analytics marts schema | Brent and WTI are preserved through staging; marts derive Brent-WTI spread at `year_month` grain. |
| Semantic marts | `mart_global_daily_market_signal` and monthly macro consumers | day or month grain | dbt-managed | analytics marts schema | Daily Brent signal is used beside daily chokepoint signal. |

## Contract Decisions

- The canonical cloud source is curated silver parquet, not bronze CSV directly.
- `BRENT_EU` remains the primary Brent benchmark used for level, momentum, and daily signal context.
- `WTI_US` is preserved alongside `BRENT_EU` so marts can derive a Brent-WTI spread joined by `year_month`.
- Brent and WTI are treated as global macro benchmarks, not reporter-specific series.

## Minimum Required Daily Fields

- `trade_date`
- `year`
- `month`
- `day`
- `year_month`
- `benchmark_code`
- `benchmark_name`
- `region`
- `source_series_id`
- `price_usd_per_bbl`
- `load_ts`

## Minimum Required Monthly Fields

- `year_month`
- `month_start_date`
- `year`
- `month`
- `benchmark_code`
- `benchmark_name`
- `region`
- `source_series_id`
- `avg_price_usd_per_bbl`
- `min_price_usd_per_bbl`
- `max_price_usd_per_bbl`
- `month_start_price_usd_per_bbl`
- `month_end_price_usd_per_bbl`
- `mom_abs_change_usd`
- `mom_pct_change`
- `trading_day_count`

## Downstream Use

- `mart_macro_monthly_features` joins Brent monthly to FX monthly.
- `mart_macro_monthly_features` derives `brent_wti_spread_usd` from `BRENT_EU - WTI_US`.
- `mart_reporter_month_macro_features` broadcasts macro monthly context to reporter-month rows.
- `mart_reporter_month_chokepoint_exposure_with_brent` appends Brent, WTI, and Brent-WTI spread context to reporter-chokepoint exposure.
- `mart_global_daily_market_signal` exposes daily Brent price, returns, rolling baselines, and z-scores beside daily chokepoint market signal fields.

## Current Operational Logging

Implemented logs and manifests exist for:

- extract: `logs/brent/brent_extract_manifest.jsonl`
- silver: `logs/brent/brent_silver_manifest.jsonl`
- GCS publish: `logs/brent/publish_brent_to_gcs.log`, `logs/brent/publish_brent_to_gcs_manifest.jsonl`
- BigQuery load: `logs/brent/load_brent_to_bigquery.log`, `logs/brent/load_brent_to_bigquery_manifest.jsonl`

## Known Gaps

- Brent is context data. It is surfaced through macro and daily signal marts rather than a dedicated Brent-only dashboard contract.
