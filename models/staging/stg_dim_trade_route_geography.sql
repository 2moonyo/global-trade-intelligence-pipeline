select
  {{ hash_text(
      "coalesce(" ~ canonical_country_iso3('reporter_iso3') ~ ", '') || '|' || "
      "coalesce(" ~ canonical_country_iso3('partner_iso3') ~ ", '') || '|' || "
      "coalesce(" ~ canonical_country_iso3('partner2_iso3') ~ ", '') || '|' || "
      "lower(trim(coalesce(route_scenario, '')))"
    ) }} as trade_route_id,
  {{ canonical_country_iso3('reporter_iso3') }} as reporter_iso3,
  {{ canonical_country_iso3('partner_iso3') }} as partner_iso3,
  {{ canonical_country_iso3('partner2_iso3') }} as partner2_iso3,
  {{ cast_string('reporter_port') }} as reporter_port,
  {{ cast_string('partner_port') }} as partner_port,
  {{ canonical_country_iso3('reporter_gateway_iso3') }} as reporter_gateway_iso3,
  {{ canonical_country_iso3('partner_gateway_iso3') }} as partner_gateway_iso3,
  {{ cast_string('reporter_basin') }} as reporter_basin,
  {{ cast_string('partner_basin') }} as partner_basin,
  {{ canonicalize_chokepoint_name('first_chokepoint') }} as first_chokepoint,
  {{ canonicalize_chokepoint_name('last_chokepoint') }} as last_chokepoint,
  {{ canonicalize_chokepoint_name('main_chokepoint') }} as main_chokepoint,
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
  {{ canonical_country_iso3('hub_iso3') }} as hub_iso3,
  {{ cast_string('hub_basin') }} as hub_basin,
  route_path_wkb,
  {{ geography_from_wkb('route_path_wkb') }} as route_path_geog
from {{ source('raw', 'dim_trade_routes') }}
