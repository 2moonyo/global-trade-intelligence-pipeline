
  
    

    create or replace table `capfractal`.`analytics_marts`.`mart_trade_route_geography`
      
    
    

    
    OPTIONS()
    as (
      with route_base as (
  select
    trade_route_id,
    reporter_iso3,
    partner_iso3,
    partner2_iso3,
    coalesce(reporter_gateway_iso3, reporter_iso3) as reporter_port_iso3,
    coalesce(partner_gateway_iso3, partner_iso3) as partner_port_iso3,
    reporter_port,
    partner_port,
    reporter_gateway_iso3,
    partner_gateway_iso3,
    reporter_basin,
    partner_basin,
    first_chokepoint,
    last_chokepoint,
    main_chokepoint,
    route_group,
    route_mode,
    route_status,
    route_basis,
    route_basis_detail,
    internal_exit_port,
    route_confidence,
    route_applicability_status,
    transport_evidence,
    routing_decision,
    route_scenario,
    distance_km,
    sea_distance_km,
    sea_distance_direct_km,
    sea_distance_forced_km,
    used_transshipment_hub,
    hub_port,
    hub_iso3,
    hub_basin,
    route_path_geog
  from `capfractal`.`analytics_staging`.`stg_dim_trade_route_geography`
),
joined as (
  select
    rb.trade_route_id,
    rb.reporter_iso3 as reporter_country_code,
    reporter_country.country_name as reporter_country_name,
    rb.partner_iso3 as partner_country_code,
    partner_country.country_name as partner_country_name,
    rb.partner2_iso3 as hub_partner_country_code,
    rb.reporter_port_iso3,
    rb.partner_port_iso3,
    rb.reporter_port,
    reporter_port_dim.port_id as reporter_port_id,
    reporter_port_dim.port_point_geog as reporter_port_geog,
    rb.partner_port,
    partner_port_dim.port_id as partner_port_id,
    partner_port_dim.port_point_geog as partner_port_geog,
    rb.reporter_gateway_iso3,
    rb.partner_gateway_iso3,
    rb.reporter_basin,
    rb.partner_basin,
    rb.main_chokepoint as chokepoint_name,
    chokepoint_dim.chokepoint_id,
    chokepoint_dim.chokepoint_kind,
    chokepoint_dim.chokepoint_point_geog,
    chokepoint_dim.zone_of_influence_geog,
    rb.first_chokepoint,
    rb.last_chokepoint,
    rb.route_group,
    rb.route_mode,
    rb.route_status,
    rb.route_basis,
    rb.route_basis_detail,
    rb.internal_exit_port,
    rb.route_confidence,
    rb.route_applicability_status,
    rb.transport_evidence,
    rb.routing_decision,
    rb.route_scenario,
    rb.distance_km,
    rb.sea_distance_km,
    rb.sea_distance_direct_km,
    rb.sea_distance_forced_km,
    rb.used_transshipment_hub,
    rb.hub_port,
    hub_port_dim.port_id as hub_port_id,
    hub_port_dim.port_point_geog as hub_port_geog,
    rb.hub_iso3,
    hub_country.country_name as hub_country_name,
    rb.hub_basin,
    rb.route_path_geog
  from route_base as rb
  left join `capfractal`.`analytics_marts`.`dim_country` as reporter_country
    on rb.reporter_iso3 = reporter_country.iso3
  left join `capfractal`.`analytics_marts`.`dim_country` as partner_country
    on rb.partner_iso3 = partner_country.iso3
  left join `capfractal`.`analytics_marts`.`dim_country` as hub_country
    on rb.hub_iso3 = hub_country.iso3
  left join `capfractal`.`analytics_staging`.`stg_dim_country_ports` as reporter_port_dim
    on rb.reporter_port_iso3 = reporter_port_dim.iso3
   and rb.reporter_port = reporter_port_dim.port_name
  left join `capfractal`.`analytics_staging`.`stg_dim_country_ports` as partner_port_dim
    on rb.partner_port_iso3 = partner_port_dim.iso3
   and rb.partner_port = partner_port_dim.port_name
  left join `capfractal`.`analytics_staging`.`stg_dim_country_ports` as hub_port_dim
    on rb.hub_iso3 = hub_port_dim.iso3
   and rb.hub_port = hub_port_dim.port_name
  left join `capfractal`.`analytics_marts`.`dim_chokepoint` as chokepoint_dim
    on rb.main_chokepoint = chokepoint_dim.chokepoint_name
)

select
  trade_route_id,
  reporter_country_code,
  reporter_country_name,
  partner_country_code,
  partner_country_name,
  hub_partner_country_code,
  reporter_port_iso3,
  reporter_port,
  reporter_port_id,
  reporter_port_geog,
  partner_port_iso3,
  partner_port,
  partner_port_id,
  partner_port_geog,
  reporter_gateway_iso3,
  partner_gateway_iso3,
  reporter_basin,
  partner_basin,
  chokepoint_id,
  chokepoint_name,
  chokepoint_kind,
  chokepoint_point_geog,
  zone_of_influence_geog,
  first_chokepoint,
  last_chokepoint,
  route_group,
  route_mode,
  route_status,
  route_basis,
  route_basis_detail,
  internal_exit_port,
  route_confidence,
  route_applicability_status,
  transport_evidence,
  routing_decision,
  route_scenario,
  distance_km,
  sea_distance_km,
  sea_distance_direct_km,
  sea_distance_forced_km,
  used_transshipment_hub,
  hub_port,
  hub_port_id,
  hub_port_geog,
  hub_iso3,
  hub_country_name,
  hub_basin,
  route_path_geog
from joined
    );
  