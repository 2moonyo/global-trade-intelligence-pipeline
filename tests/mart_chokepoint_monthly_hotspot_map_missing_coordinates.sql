select
  count(*) as rows_without_map_coordinates
from {{ ref('mart_chokepoint_monthly_hotspot_map') }}
where has_map_coordinates_flag = false
having count(*) > 0
