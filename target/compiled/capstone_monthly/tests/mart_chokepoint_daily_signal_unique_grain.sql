-- Fails when mart_chokepoint_daily_signal has duplicate rows at its declared grain.
with duplicate_grain as (
  select
    date_day,
    chokepoint_id,
    count(*) as row_count
  from `capfractal`.`analytics_marts`.`mart_chokepoint_daily_signal`
  group by 1, 2
  having count(*) > 1
)

select *
from duplicate_grain