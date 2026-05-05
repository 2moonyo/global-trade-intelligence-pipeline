select
  chokepoint_id,
  chokepoint_name,
  longitude,
  latitude,
  geo_point,
  has_map_coordinates_flag
from {{ ref('mart_chokepoint_monthly_hotspot_map') }}
where longitude is null
   or latitude is null
   or geo_point is null
   or not coalesce(has_map_coordinates_flag, false)
