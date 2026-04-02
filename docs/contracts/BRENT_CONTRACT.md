# Brent Contract

See also:

- [SHARED_WAREHOUSE_AND_SERVING_CONTRACT.md](./SHARED_WAREHOUSE_AND_SERVING_CONTRACT.md)

## Status

- Bronze ingest is scripted with per-run logs and manifest output
- Curated silver daily and monthly parquet partitions exist and are used by the local warehouse and cloud slice
- BigQuery raw landing is provisioned for both daily and monthly Brent tables

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
| Silver curated daily | `brent_daily` | one row per trading date x benchmark | `year=YYYY/month=MM` | `data/silver/brent/brent_daily/year=YYYY/month=MM/brent_daily.parquet` | Built by `ingest/fred/brent_silver.py`. A consolidated compatibility snapshot is also written to `data/silver/brent/brent_daily.parquet`. |
| Silver curated monthly | `brent_monthly` | one row per `year_month` x benchmark | `year=YYYY/month=MM` | `data/silver/brent/brent_monthly/year=YYYY/month=MM/brent_monthly.parquet` | Built by `ingest/fred/brent_silver.py`. A consolidated compatibility snapshot is also written to `data/silver/brent/brent_monthly.parquet`. |
| GCS silver publish | Brent daily and monthly parquet partitions | table-specific | `year=YYYY/month=MM` | `gs://<bucket>/<prefix>/silver/brent/*` | Published by `warehouse/publish_brent_to_gcs.py` with checksum-aware skipping. |
| BigQuery raw landing | `raw.brent_daily`, `raw.brent_monthly` | table-specific | date partitioned by table grain | `raw.*` | Loaded by `warehouse/load_brent_to_bigquery.py` with month-level load state and checksum-aware reload skipping. |
| DuckDB raw landing | `raw.brent_daily`, `raw.brent_monthly` | table-specific | DuckDB tables | `raw.*` | Loaded directly from partitioned silver parquet. |
| dbt staging | `stg_brent_monthly` | one row per `year_month` x benchmark | dbt-managed | analytics schemas | Standardizes monthly series for marts. |
| dbt marts | `mart_macro_monthly_features`, `mart_reporter_month_macro_features`, `mart_reporter_month_chokepoint_exposure_with_brent` | mart-specific | dbt-managed | analytics schemas | Brent and WTI are preserved through staging; marts derive a Brent-WTI spread at `year_month` grain. |
| Dashboard | indirect only | none | page-level | `app/` | Brent is not yet a dedicated Streamlit page, but it is available in downstream marts. |

## Contract Decisions

- The canonical local warehouse source is the curated silver monthly parquet, not the bronze CSV directly.
- `BRENT_EU` remains the primary Brent benchmark used for level and momentum context in downstream marts.
- `WTI_US` is preserved alongside `BRENT_EU` so marts can derive a monthly Brent-WTI spread joined by `year_month`.
- Brent and WTI are treated as global monthly macro benchmarks, not reporter-specific series.

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

## Contract Notes

- Both `BRENT_EU` and `WTI_US` are preserved in raw and staging data.
- Brent-WTI spread is intentionally not stored in raw or staging. It is derived in marts from the two benchmark rows at monthly grain.

## Downstream Use

- `mart_macro_monthly_features` joins Brent monthly to FX monthly
- `mart_macro_monthly_features` derives `brent_wti_spread_usd` from `BRENT_EU - WTI_US`
- `mart_reporter_month_macro_features` broadcasts macro monthly context to reporter-month rows
- `mart_reporter_month_chokepoint_exposure_with_brent` appends Brent, WTI, and Brent-WTI spread context to reporter-chokepoint exposure

## Known Gaps

- Brent is analytically present but not yet prominently surfaced in the current Streamlit narrative.
