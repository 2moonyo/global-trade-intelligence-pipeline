-- Test: previous-month fields should only be populated when an adjacent prior month exists.

select
  *
from `chokepoint-capfractal`.`analytics_marts`.`mart_dashboard_global_trade_overview`
where not previous_month_available_flag
  and (
    previous_month_total_trade_value_usd is not null
    or previous_month_import_trade_value_usd is not null
    or previous_month_export_trade_value_usd is not null
    or total_trade_value_mom_change_usd is not null
    or import_trade_value_mom_change_usd is not null
    or export_trade_value_mom_change_usd is not null
    or total_trade_value_mom_change_pct is not null
    or import_trade_value_mom_change_pct is not null
    or export_trade_value_mom_change_pct is not null
  )