

  create or replace view `capfractal`.`analytics_staging`.`stg_brent_daily`
  OPTIONS()
  as -- Grain: one row per date_day + benchmark_code.
-- Deduplicates the raw Brent daily landing table to the latest loaded record
-- for each benchmark trading day.

with raw_brent as (
  select
    cast(trade_date as date) as date_day,
    cast(year_month as string) as year_month,
    
    safe_cast(year as INT64)
   as year,
    
    safe_cast(month as INT64)
   as month,
    
    safe_cast(day as INT64)
   as day,
    cast(benchmark_code as string) as benchmark_code,
    cast(benchmark_name as string) as benchmark_name,
    cast(region as string) as region,
    cast(source_series_id as string) as source_series_id,
    
    safe_cast(price_usd_per_bbl as FLOAT64)
   as price_usd_per_bbl,
    cast(load_ts as timestamp) as load_ts
  from `capfractal`.`raw`.`brent_daily`
  where trade_date is not null
    and benchmark_code is not null
),
ranked as (
  select
    *,
    row_number() over (
      partition by date_day, benchmark_code
      order by load_ts desc
    ) as load_rank
  from raw_brent
)

select
  date_day,
  year_month,
  year,
  month,
  day,
  benchmark_code,
  benchmark_name,
  region,
  source_series_id,
  price_usd_per_bbl,
  load_ts
from ranked
where load_rank = 1;

