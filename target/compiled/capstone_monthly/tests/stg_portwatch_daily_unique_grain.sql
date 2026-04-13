-- Fails when stg_portwatch_daily has duplicate rows at its declared grain.
with duplicate_grain as (
  select
    date_day,
    chokepoint_id,
    count(*) as row_count
  from `fullcap-10111`.`analytics_staging`.`stg_portwatch_daily`
  group by 1, 2
  having count(*) > 1
)

select *
from duplicate_grain