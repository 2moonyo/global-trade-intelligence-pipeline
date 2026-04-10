-- Test: latest_month_flag must only be true on the maximum month_start_date.

with latest_month as (
  select
    max(month_start_date) as latest_month_start_date
  from {{ ref('mart_dashboard_global_trade_overview') }}
)

select
  mart.*
from {{ ref('mart_dashboard_global_trade_overview') }} as mart
cross join latest_month as lm
where (mart.month_start_date = lm.latest_month_start_date and not mart.latest_month_flag)
   or (mart.month_start_date <> lm.latest_month_start_date and mart.latest_month_flag)
