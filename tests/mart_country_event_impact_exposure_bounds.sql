-- Test: event exposure percentage should remain within 0-100 bounds.

select
  reporter_country_code,
  event_id,
  year_month_key,
  event_exposure_pct
from {{ ref('mart_country_event_impact') }}
where event_exposure_pct < 0
   or event_exposure_pct > 100
