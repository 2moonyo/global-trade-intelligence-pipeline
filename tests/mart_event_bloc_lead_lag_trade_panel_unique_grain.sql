select
  event_id,
  bloc_code,
  relative_month_offset,
  count(*) as row_count
from {{ ref('mart_event_bloc_lead_lag_trade_panel') }}
group by 1, 2, 3
having count(*) > 1
