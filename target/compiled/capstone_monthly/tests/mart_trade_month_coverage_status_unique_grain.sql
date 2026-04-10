select
  month_start_date,
  count(*) as row_count
from `capfractal`.`analytics_marts`.`mart_trade_month_coverage_status`
group by 1
having count(*) > 1