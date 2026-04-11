select
  month_start_date,
  reporter_iso3,
  count(*) as row_count
from `chokepoint-capfractal`.`analytics_marts`.`mart_reporter_month_exposure_map`
group by 1, 2
having count(*) > 1