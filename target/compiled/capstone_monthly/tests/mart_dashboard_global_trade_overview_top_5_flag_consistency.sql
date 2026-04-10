-- Test: top-5 flag must align exactly with ranked reporters that have upstream trade data.

select
  *
from `capfractal`.`analytics_marts`.`mart_dashboard_global_trade_overview`
where (
  top_5_reporter_in_month_flag
  and (
    not has_reported_trade_data_flag
    or reporter_rank_by_total_trade_in_month is null
    or reporter_rank_by_total_trade_in_month > 5
  )
)
or (
  has_reported_trade_data_flag
  and reporter_rank_by_total_trade_in_month <= 5
  and not top_5_reporter_in_month_flag
)