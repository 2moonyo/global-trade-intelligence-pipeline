-- Grain: one row per date_day + benchmark_code.
-- Deduplicates the raw Brent daily landing table to the latest loaded record
-- for each benchmark trading day.

with raw_brent as (
  select
    cast(trade_date as date) as date_day,
    {{ cast_string('year_month') }} as year_month,
    {{ safe_cast('year', dbt.type_int()) }} as year,
    {{ safe_cast('month', dbt.type_int()) }} as month,
    {{ safe_cast('day', dbt.type_int()) }} as day,
    {{ cast_string('benchmark_code') }} as benchmark_code,
    {{ cast_string('benchmark_name') }} as benchmark_name,
    {{ cast_string('region') }} as region,
    {{ cast_string('source_series_id') }} as source_series_id,
    {{ safe_cast('price_usd_per_bbl', dbt.type_float()) }} as price_usd_per_bbl,
    cast(load_ts as timestamp) as load_ts
  from {{ source('raw', 'brent_daily') }}
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
where load_rank = 1
