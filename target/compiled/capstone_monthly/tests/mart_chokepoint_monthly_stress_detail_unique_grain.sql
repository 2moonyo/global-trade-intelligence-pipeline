select
  month_start_date,
  chokepoint_id,
  count(*) as row_count
from `chokepoint-capfractal`.`analytics_marts`.`mart_chokepoint_monthly_stress_detail`
group by 1, 2
having count(*) > 1