-- Fails when mart_global_daily_market_signal has duplicate rows at its declared grain.
with duplicate_grain as (
  select
    date_day,
    count(*) as row_count
  from {{ ref('mart_global_daily_market_signal') }}
  group by 1
  having count(*) > 1
)

select *
from duplicate_grain
