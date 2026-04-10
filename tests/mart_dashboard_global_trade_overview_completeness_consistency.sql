-- Test: complete_month_flag must exactly match month-level reporter coverage equality.

select
  *
from {{ ref('mart_dashboard_global_trade_overview') }}
where (
  complete_month_flag
  and reporters_with_data_in_month <> expected_reporter_count
)
or (
  not complete_month_flag
  and reporters_with_data_in_month = expected_reporter_count
)
