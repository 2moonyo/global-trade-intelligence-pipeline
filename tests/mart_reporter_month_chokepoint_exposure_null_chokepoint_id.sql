select
  reporter_iso3,
  year_month,
  chokepoint_name
from {{ ref('mart_reporter_month_chokepoint_exposure') }}
where chokepoint_id is null
