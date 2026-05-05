select
  chokepoint_id,
  month_start_date,
  latitude,
  longitude,
  lat_lng_string
from {{ ref('mart_chokepoint_monthly_hotspot_map') }}
where lat_lng_string is null
