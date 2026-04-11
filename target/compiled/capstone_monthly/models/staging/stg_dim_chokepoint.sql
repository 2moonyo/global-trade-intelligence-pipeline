select
  
    to_hex(md5(cast(lower(trim(chokepoint_name)) as string)))
   as chokepoint_id,
  cast(chokepoint_name as string) as chokepoint_name,
  cast(kind as string) as chokepoint_kind,
  cast(longitude as FLOAT64) as longitude,
  cast(latitude as FLOAT64) as latitude,
  cast(zone_of_influence_radius_m as INT64) as zone_of_influence_radius_m,
  chokepoint_point_wkb,
  zone_of_influence_wkb,
  
    ST_GEOGFROMWKB(chokepoint_point_wkb)
   as chokepoint_point_geog,
  
    ST_GEOGFROMWKB(zone_of_influence_wkb)
   as zone_of_influence_geog
from `chokepoint-capfractal`.`raw`.`dim_chokepoint`