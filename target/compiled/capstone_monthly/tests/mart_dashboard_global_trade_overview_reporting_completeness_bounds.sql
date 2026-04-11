-- Test: reporting completeness ratio must remain between 0 and 1.

select
  *
from `chokepoint-capfractal`.`analytics_marts`.`mart_dashboard_global_trade_overview`
where reporting_completeness_pct < 0
   or reporting_completeness_pct > 1