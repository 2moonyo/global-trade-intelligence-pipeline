select
  {{ hash_text('lower(trim(chokepoint_name))') }} as chokepoint_id,
  {{ cast_string('chokepoint_name') }} as chokepoint_name,
  {{ cast_string('kind') }} as chokepoint_kind,
  {{ cast_float('longitude') }} as longitude,
  {{ cast_float('latitude') }} as latitude,
  {{ cast_int('zone_of_influence_radius_m') }} as zone_of_influence_radius_m,
  chokepoint_point_wkb,
  zone_of_influence_wkb,
  {{ geography_from_wkb('chokepoint_point_wkb') }} as chokepoint_point_geog,
  {{ geography_from_wkb('zone_of_influence_wkb') }} as zone_of_influence_geog
from {{ source('raw', 'dim_chokepoint') }}
