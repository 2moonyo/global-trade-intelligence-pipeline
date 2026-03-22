
  
    
    

    create  table
      "analytics"."analytics_marts"."fct_reporter_partner_commodity_route_month__dbt_tmp"
  
    as (
      with route_candidates as (
  select
    upper(trim(reporter_iso3)) as reporter_iso3,
    upper(trim(partner_iso3)) as partner_iso3,
    main_chokepoint,
    route_group,
    route_mode,
    route_basis,
    route_confidence,
    route_applicability_status,
    route_scenario,
    sea_distance_km,
    sea_distance_direct_km,
    sea_distance_forced_km,
    reporter_port,
    partner_port,
    reporter_basin,
    partner_basin,
    used_transshipment_hub,
    hub_port,
    hub_iso3,
    hub_basin,
    row_number() over (
      partition by upper(trim(reporter_iso3)), upper(trim(partner_iso3))
      order by
        case
          when lower(trim(route_scenario)) = 'default_shortest' then 0
          else 1
        end,
        route_scenario
    ) as _rn
  from "analytics"."raw"."dim_trade_routes"
),
route_map as (
  select
    reporter_iso3,
    partner_iso3,
    main_chokepoint,
    route_group,
    route_mode,
    route_basis,
    route_confidence,
    route_applicability_status,
    route_scenario,
    sea_distance_km,
    sea_distance_direct_km,
    sea_distance_forced_km,
    reporter_port,
    partner_port,
    reporter_basin,
    partner_basin,
    used_transshipment_hub,
    hub_port,
    hub_iso3,
    hub_basin
  from route_candidates
  where _rn = 1
)

select
  f.reporter_iso3,
  f.partner_iso3,
  f.cmd_code,
  f.period,
  f.year_month,
  f.ref_year,
  f.trade_flow,
  f.trade_value_usd,
  f.net_weight_kg,
  f.gross_weight_kg,
  f.qty,
  f.usd_per_kg,
  f.record_count,
  rm.main_chokepoint,
  rm.route_group,
  rm.route_mode,
  rm.route_basis,
  rm.route_confidence,
  coalesce(rm.route_applicability_status, ra.route_applicability_status) as route_applicability_status,
  rm.route_scenario,
  rm.sea_distance_km,
  rm.sea_distance_direct_km,
  rm.sea_distance_forced_km,
  rm.reporter_port,
  rm.partner_port,
  rm.reporter_basin,
  rm.partner_basin,
  rm.used_transshipment_hub,
  rm.hub_port,
  rm.hub_iso3,
  rm.hub_basin
from "analytics"."analytics_marts"."fct_reporter_partner_commodity_month" as f
left join route_map as rm
  on f.reporter_iso3 = rm.reporter_iso3
 and f.partner_iso3 = rm.partner_iso3
left join "analytics"."analytics_staging"."stg_route_applicability" as ra
  on f.reporter_iso3 = ra.reporter_iso3
 and f.partner_iso3 = ra.partner_iso3
    );
  
  