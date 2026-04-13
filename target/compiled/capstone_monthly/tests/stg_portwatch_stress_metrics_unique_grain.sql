select
  chokepoint_id,
  year_month,
  count(*) as row_count
from `fullcap-10111`.`analytics_staging`.`stg_portwatch_stress_metrics`
group by 1, 2
having count(*) > 1