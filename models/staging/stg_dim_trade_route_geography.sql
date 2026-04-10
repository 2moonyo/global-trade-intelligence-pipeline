select
  {{ hash_text(
      "upper(trim(coalesce(reporter_iso3, ''))) || '|' || "
      "upper(trim(coalesce(partner_iso3, ''))) || '|' || "
      "upper(trim(coalesce(partner2_iso3, ''))) || '|' || "
      "lower(trim(coalesce(route_scenario, '')))"
    ) }} as trade_route_id,
  upper(trim({{ cast_string('reporter_iso3') }})) as reporter_iso3,
  upper(trim({{ cast_string('partner_iso3') }})) as partner_iso3,
  nullif(upper(trim({{ cast_string('partner2_iso3') }})), '') as partner2_iso3,
  {{ cast_string('reporter_port') }} as reporter_port,
  {{ cast_string('partner_port') }} as partner_port,
  nullif(upper(trim({{ cast_string('reporter_gateway_iso3') }})), '') as reporter_gateway_iso3,
  nullif(upper(trim({{ cast_string('partner_gateway_iso3') }})), '') as partner_gateway_iso3,
  {{ cast_string('reporter_basin') }} as reporter_basin,
  {{ cast_string('partner_basin') }} as partner_basin,
  {{ cast_string('first_chokepoint') }} as first_chokepoint,
  {{ cast_string('last_chokepoint') }} as last_chokepoint,
  {{ cast_string('main_chokepoint') }} as main_chokepoint,
  {{ cast_string('route_group') }} as route_group,
  {{ cast_string('route_mode') }} as route_mode,
  {{ cast_string('route_status') }} as route_status,
  {{ cast_string('route_basis') }} as route_basis,
  {{ cast_string('route_basis_detail') }} as route_basis_detail,
  {{ cast_string('internal_exit_port') }} as internal_exit_port,
  {{ cast_string('route_confidence') }} as route_confidence,
  {{ cast_string('route_applicability_status') }} as route_applicability_status,
  {{ cast_string('transport_evidence') }} as transport_evidence,
  {{ cast_string('routing_decision') }} as routing_decision,
  {{ cast_string('route_scenario') }} as route_scenario,
  {{ cast_float('distance_km') }} as distance_km,
  {{ cast_float('sea_distance_km') }} as sea_distance_km,
  {{ cast_float('sea_distance_direct_km') }} as sea_distance_direct_km,
  {{ cast_float('sea_distance_forced_km') }} as sea_distance_forced_km,
  cast(used_transshipment_hub as boolean) as used_transshipment_hub,
  {{ cast_string('hub_port') }} as hub_port,
  nullif(upper(trim({{ cast_string('hub_iso3') }})), '') as hub_iso3,
  {{ cast_string('hub_basin') }} as hub_basin,
  route_path_wkb,
  {{ geography_from_wkb('route_path_wkb') }} as route_path_geog
from {{ source('raw', 'dim_trade_routes') }}
