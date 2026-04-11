select *
from `chokepoint-capfractal`.`analytics_marts`.`mart_trade_month_coverage_status`
where reporting_completeness_pct < 0
   or reporting_completeness_pct > 1
   or missing_reporter_pct < 0
   or missing_reporter_pct > 1