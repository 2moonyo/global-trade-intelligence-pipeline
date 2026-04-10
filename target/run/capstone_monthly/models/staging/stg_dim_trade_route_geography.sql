

  create or replace view `capfractal`.`analytics_staging`.`stg_dim_trade_route_geography`
  OPTIONS()
  as select
  
    to_hex(md5(cast(upper(trim(coalesce(reporter_iso3, ''))) || '|' || upper(trim(coalesce(partner_iso3, ''))) || '|' || upper(trim(coalesce(partner2_iso3, ''))) || '|' || lower(trim(coalesce(route_scenario, ''))) as string)))
   as trade_route_id,
  upper(trim(cast(reporter_iso3 as string))) as reporter_iso3,
  upper(trim(cast(partner_iso3 as string))) as partner_iso3,
  nullif(upper(trim(cast(partner2_iso3 as string))), '') as partner2_iso3,
  cast(reporter_port as string) as reporter_port,
  cast(partner_port as string) as partner_port,
  nullif(upper(trim(cast(reporter_gateway_iso3 as string))), '') as reporter_gateway_iso3,
  nullif(upper(trim(cast(partner_gateway_iso3 as string))), '') as partner_gateway_iso3,
  cast(reporter_basin as string) as reporter_basin,
  cast(partner_basin as string) as partner_basin,
  cast(first_chokepoint as string) as first_chokepoint,
  cast(last_chokepoint as string) as last_chokepoint,
  cast(main_chokepoint as string) as main_chokepoint,
  cast(route_group as string) as route_group,
  cast(route_mode as string) as route_mode,
  cast(route_status as string) as route_status,
  cast(route_basis as string) as route_basis,
  cast(route_basis_detail as string) as route_basis_detail,
  cast(internal_exit_port as string) as internal_exit_port,
  cast(route_confidence as string) as route_confidence,
  cast(route_applicability_status as string) as route_applicability_status,
  cast(transport_evidence as string) as transport_evidence,
  cast(routing_decision as string) as routing_decision,
  cast(route_scenario as string) as route_scenario,
  cast(distance_km as FLOAT64) as distance_km,
  cast(sea_distance_km as FLOAT64) as sea_distance_km,
  cast(sea_distance_direct_km as FLOAT64) as sea_distance_direct_km,
  cast(sea_distance_forced_km as FLOAT64) as sea_distance_forced_km,
  cast(used_transshipment_hub as boolean) as used_transshipment_hub,
  cast(hub_port as string) as hub_port,
  nullif(upper(trim(cast(hub_iso3 as string))), '') as hub_iso3,
  cast(hub_basin as string) as hub_basin,
  route_path_wkb,
  
    ST_GEOGFROMWKB(route_path_wkb)
   as route_path_geog
from `capfractal`.`raw`.`dim_trade_routes`;

