-- Test: mart_dashboard_global_trade_overview must be unique at reporter_country_code + month_start_date.

select
  reporter_country_code,
  month_start_date,
  count(*) as row_count
from `capfractal`.`analytics_marts`.`mart_dashboard_global_trade_overview`
group by 1, 2
having count(*) > 1