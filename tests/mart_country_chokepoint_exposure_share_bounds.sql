-- Test: chokepoint exposure percentages should remain within 0-100 bounds.

select
  reporter_country_code,
  chokepoint_id,
  year_month_key,
  chokepoint_exposure_pct,
  high_confidence_share_pct,
  medium_confidence_share_pct
from {{ ref('mart_country_chokepoint_exposure') }}
where chokepoint_exposure_pct < 0
   or chokepoint_exposure_pct > 100
   or high_confidence_share_pct < 0
   or high_confidence_share_pct > 100
   or medium_confidence_share_pct < 0
   or medium_confidence_share_pct > 100
