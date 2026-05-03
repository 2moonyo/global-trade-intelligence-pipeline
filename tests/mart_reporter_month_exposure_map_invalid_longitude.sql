select
  reporter_iso3,
  month_start_date,
  longitude
from {{ ref('mart_reporter_month_exposure_map') }}
where longitude is not null
  and (longitude < -180 or longitude > 180)
