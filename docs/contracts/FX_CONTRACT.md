# FX Contract

See also:

- [SHARED_WAREHOUSE_AND_SERVING_CONTRACT.md](./SHARED_WAREHOUSE_AND_SERVING_CONTRACT.md)

## Status

- Bronze ingest is scripted
- Local DuckDB and dbt path is implemented
- No separate silver layer is currently used downstream
- No cloud landing path is provisioned yet

## Source Systems

- ECB EXR API

## Purpose

- Provide monthly FX context for macro feature modelling

## Lifecycle By Phase

| Phase | Current implemented asset | Grain | Partitioning | Canonical path or table | Notes |
| --- | --- | --- | --- | --- | --- |
| Bronze standardized | FX daily standardized bronze CSVs | one row per `dt` x `series_id` | `dt=YYYY-MM-DD` | `data/bronze/ecb_fx_eu/dt=YYYY-MM-DD/part-*.csv` | Written via `ingest/common/bronze_io.py`. |
| Bronze batch | FX batch CSV snapshot | one row per `date` x `quote_ccy` x `base_ccy` | batch file | `data/bronze/ecb_fx_eu/Batch/ecb_fx_eu_<stamp>.csv` | This is the file currently loaded into DuckDB raw. |
| DuckDB raw landing | `raw.ecb_fx_eu_daily` | one row per `date` x `quote_ccy` x `base_ccy` x `load_ts` | DuckDB table | `raw.ecb_fx_eu_daily` | Bootstrapped from `Batch/*.csv`. |
| dbt staging | `stg_fx_monthly` | one row per `year_month` x `fx_currency_code` | dbt-managed | analytics schemas | Converts quote-per-EUR into USD-per-currency monthly rates. |
| dbt marts | `mart_macro_monthly_features`, `mart_reporter_month_macro_features` | mart-specific | dbt-managed | analytics schemas | FX is part of the macro context surface. |
| Dashboard | indirect only | none | page-level | `app/` | No dedicated FX page yet. |

## Contract Decisions

- The current downstream path uses the batch CSV, not the `dt=` bronze partitions, for DuckDB bootstrap.
- The `dt=` partitions still matter as a standardized bronze interface and future cloud-ready pattern.
- Monthly FX is modeled at `year_month + fx_currency_code`.

## Monthly FX Computation

`stg_fx_monthly`:

1. Deduplicates daily rows by keeping the latest `load_ts` for each `fx_date + quote_ccy + base_ccy`
2. Identifies the USD quote for each base currency and day
3. Converts quote-per-base rates into USD-per-currency
4. Averages daily converted rates to monthly rates
5. Calculates month-over-month percentage change by currency

Justification:

- the ECB feed is quote-per-EUR-oriented
- downstream marts need a reporter-agnostic macro context benchmark in USD terms
- deduping by latest load timestamp makes re-runs safe without requiring destructive bronze cleanup

## Minimum Required Fields

Raw daily:

- `date`
- `quote_ccy`
- `base_ccy`
- `rate`
- `load_ts`

Monthly staged:

- `year_month`
- `fx_currency_code`
- `fx_rate_to_usd`
- `fx_mom_change`

## Known Gaps

- FX is not yet materialized as a silver parquet contract comparable to PortWatch or Brent.
- No dashboard page focuses on FX directly; it is currently contextual data only.
