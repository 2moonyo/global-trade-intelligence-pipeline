select
  
    to_hex(md5(cast(upper(trim(coalesce(iso3, ''))) || '|' || lower(trim(coalesce(port_name, ''))) as string)))
   as port_id,
  upper(trim(cast(iso3 as string))) as iso3,
  cast(port_name as string) as port_name,
  cast(longitude as FLOAT64) as longitude,
  cast(latitude as FLOAT64) as latitude,
  cast(world_water_body as string) as world_water_body,
  cast(port_basin as string) as port_basin,
  cast(harbor_size as string) as harbor_size,
  cast(harbor_type as string) as harbor_type,
  cast(harbor_use as string) as harbor_use,
  cast(fac_container as INT64) as fac_container,
  cast(fac_solid_bulk as INT64) as fac_solid_bulk,
  cast(fac_liquid_bulk as INT64) as fac_liquid_bulk,
  cast(fac_oil_terminal as INT64) as fac_oil_terminal,
  cast(fac_lng_terminal as INT64) as fac_lng_terminal,
  cast(port_score as FLOAT64) as port_score,
  cast(port_rank as INT64) as port_rank,
  port_point_wkb,
  
    ST_GEOGFROMWKB(port_point_wkb)
   as port_point_geog
from `capfractal`.`raw`.`dim_country_ports`