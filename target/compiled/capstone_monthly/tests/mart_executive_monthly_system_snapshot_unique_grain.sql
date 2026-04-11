select
  month_start_date,
  count(*) as row_count
from `chokepoint-capfractal`.`analytics_marts`.`mart_executive_monthly_system_snapshot`
group by 1
having count(*) > 1