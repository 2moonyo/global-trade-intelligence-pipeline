select
  event_id,
  bloc_code,
  relative_month_offset
from {{ ref('mart_event_bloc_lead_lag_trade_panel') }}
where relative_month_offset < -6
   or relative_month_offset > 6
