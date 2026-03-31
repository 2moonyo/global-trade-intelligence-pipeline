# Brent Contract

See also:

- [SHARED_WAREHOUSE_AND_SERVING_CONTRACT.md](./SHARED_WAREHOUSE_AND_SERVING_CONTRACT.md)

## Status

- Bronze ingest is scripted
- Curated silver files exist and are used by the local warehouse
- No BigQuery landing path is provisioned yet

## Source Systems

- FRED API
- Series currently used in code:
  - `DCOILBRENTEU`
  - `DCOILWTICO`
  - `POILBREUSDM`

## Purpose

- Provide monthly oil price context for macro features and chokepoint exposure overlays

## Lifecycle By Phase

| Phase | Current implemented asset | Grain | Partitioning | Canonical path or table | Notes |
| --- | --- | --- | --- | --- | --- |
| Bronze local daily | Brent daily CSV extracts | one row per `date` x `region` | `year=YYYY/month=MM/day=DD` | `data/bronze/brent/year=YYYY/month=MM/day=DD/brent_prices_YYYYMMDD.csv` | Written by `ingest/fred/brent_crude.py`. |
| Bronze local batch | Brent batch snapshot | one row per `date` x `region` | batch file | `data/bronze/brent/Batch/brent_crude_<stamp>.csv` | Kept as a compatibility reference export. |
| Silver curated daily | `brent_daily` | one row per trading date x benchmark | none | `data/silver/brent/brent_daily.parquet` | Current local raw source. |
| Silver curated monthly | `brent_monthly` | one row per `year_month` x benchmark | none | `data/silver/brent/brent_monthly.parquet` | Current local raw source. |
| DuckDB raw landing | `raw.brent_daily`, `raw.brent_monthly` | table-specific | DuckDB tables | `raw.*` | Loaded directly from silver parquet. |
| dbt staging | `stg_brent_monthly` | one row per `year_month` x benchmark | dbt-managed | analytics schemas | Standardizes monthly series for marts. |
| dbt marts | `mart_macro_monthly_features`, `mart_reporter_month_macro_features`, `mart_reporter_month_chokepoint_exposure_with_brent` | mart-specific | dbt-managed | analytics schemas | Brent is used as contextual macro and exposure overlay. |
| Dashboard | indirect only | none | page-level | `app/` | Brent is not yet a dedicated Streamlit page, but it is available in downstream marts. |

## Contract Decisions

- The canonical local warehouse source is the curated silver monthly parquet, not the bronze CSV directly.
- `BRENT_EU` is the benchmark currently selected for downstream marts.
- Brent is treated as a single monthly macro benchmark joined by `year_month`, not by reporter.

## Minimum Required Monthly Fields

- `year_month`
- `month_start_date`
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

- `mart_macro_monthly_features` joins Brent monthly to FX monthly
- `mart_reporter_month_macro_features` broadcasts macro monthly context to reporter-month rows
- `mart_reporter_month_chokepoint_exposure_with_brent` appends Brent price context to reporter-chokepoint exposure

## Known Gaps

- The silver build that creates `brent_daily.parquet` and `brent_monthly.parquet` is not yet formalized in the repo as a single standardized silver job.
- Brent is analytically present but not yet prominently surfaced in the current Streamlit narrative.
