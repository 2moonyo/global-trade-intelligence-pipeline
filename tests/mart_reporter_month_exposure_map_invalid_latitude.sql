select
  reporter_iso3,
  month_start_date,
  latitude
from {{ ref('mart_reporter_month_exposure_map') }}
where latitude is not null
  and (latitude < -90 or latitude > 90)
