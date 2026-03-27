with route_candidates as (
  select
    upper(trim(reporter_iso3)) as reporter_iso3,
    upper(trim(partner_iso3)) as partner_iso3,
    main_chokepoint,
    first_chokepoint,
    last_chokepoint,
    chokepoint_sequence_str,
    headline_exposure_group,
    route_group,
    route_mode,
    route_status,
    route_basis,
    route_confidence,
    transport_evidence,
    routing_decision,
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
    first_chokepoint,
    last_chokepoint,
    chokepoint_sequence_str,
    headline_exposure_group,
    route_group,
    route_mode,
    route_status,
    route_basis,
    route_confidence,
    transport_evidence,
    routing_decision,
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
),
pair_applicability as (
  select
    reporter_iso3,
    partner_iso3,
    bool_or(coalesce(has_sea, false)) as has_sea,
    bool_or(coalesce(has_inland_water, false)) as has_inland_water,
    bool_or(coalesce(has_unknown, false)) as has_unknown,
    bool_or(coalesce(has_non_marine, false)) as has_non_marine,
    bool_or(partner2_iso3 is not null) as has_associated_hub_route,
    case
      when bool_or(upper(trim(coalesce(route_applicability_status, ''))) = 'MARITIME_ELIGIBLE') then 'MARITIME_ELIGIBLE'
      when bool_or(upper(trim(coalesce(route_applicability_status, ''))) = 'NON_MARITIME_ONLY') then 'NON_MARITIME_ONLY'
      when bool_or(upper(trim(coalesce(route_applicability_status, ''))) = 'UNKNOWN_MOT') then 'UNKNOWN_MOT'
      when bool_or(upper(trim(coalesce(route_applicability_status, ''))) = 'NO_MOT_DATA') then 'NO_MOT_DATA'
      else null
    end as pair_route_applicability_status
  from "analytics"."analytics_staging"."stg_route_applicability"
  group by 1, 2
),
base_fact as (
  select
    f.canonical_grain_key,
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
    rm.first_chokepoint,
    rm.last_chokepoint,
    rm.chokepoint_sequence_str,
    rm.headline_exposure_group,
    rm.route_group,
    rm.route_mode,
    rm.route_status,
    rm.route_basis,
    rm.route_confidence,
    rm.transport_evidence,
    rm.routing_decision,
    coalesce(rm.route_applicability_status, pa.pair_route_applicability_status) as route_applicability_status,
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
    rm.hub_basin,
    pa.has_sea,
    pa.has_inland_water,
    pa.has_unknown,
    pa.has_non_marine,
    pa.has_associated_hub_route,
    case
      when lower(trim(coalesce(rm.route_status, ''))) = 'routed' then true
      when lower(trim(coalesce(rm.routing_decision, ''))) in ('route_by_observed_sea', 'route_by_inference') then true
      when upper(trim(coalesce(rm.route_applicability_status, pa.pair_route_applicability_status, ''))) = 'MARITIME_ELIGIBLE' then true
      else false
    end as _is_maritime_routed_base
  from "analytics"."analytics_marts"."fct_reporter_partner_commodity_month" as f
  -- Pair-level motCode evidence comes from route applicability and is used for routing confidence gating.
  left join route_map as rm
    on f.reporter_iso3 = rm.reporter_iso3
   and f.partner_iso3 = rm.partner_iso3
  left join pair_applicability as pa
    on f.reporter_iso3 = pa.reporter_iso3
   and f.partner_iso3 = pa.partner_iso3
)

select
  b.canonical_grain_key,
  b.reporter_iso3,
  b.partner_iso3,
  b.cmd_code,
  b.period,
  b.year_month,
  b.ref_year,
  b.trade_flow,
  b.trade_value_usd,
  b.net_weight_kg,
  b.gross_weight_kg,
  b.qty,
  b.usd_per_kg,
  b.record_count,
  b.main_chokepoint,
  b.first_chokepoint,
  b.last_chokepoint,
  b.chokepoint_sequence_str,
  b.headline_exposure_group,
  b.route_group,
  b.route_mode,
  b.route_status,
  b.route_basis,
  b.route_confidence,
  b.transport_evidence,
  b.routing_decision,
  b.route_applicability_status,
  case
    when coalesce(b.has_non_marine, false) and not coalesce(b.has_sea, false) and not coalesce(b.has_inland_water, false)
      then 'NON_MARITIME_ONLY'
    when coalesce(b.has_unknown, false)
      then 'UNKNOWN_MOT'
    when coalesce(b.has_sea, false) or coalesce(b.has_inland_water, false)
      then 'MARITIME_EVIDENCE'
    else 'NO_MOT_DATA'
  end as mot_code_filter_status,
  case
    when coalesce(b.has_non_marine, false) and not coalesce(b.has_sea, false) and not coalesce(b.has_inland_water, false)
      then false
    else b._is_maritime_routed_base
  end as is_maritime_routed,
  case
    when coalesce(b.has_non_marine, false) and not coalesce(b.has_sea, false) and not coalesce(b.has_inland_water, false)
      then 'VERY_LOW'
    when coalesce(b.has_unknown, false)
      then 'LOW'
    when not b._is_maritime_routed_base
      then 'LOW'
    when lower(trim(coalesce(b.route_confidence, ''))) in ('high', 'very_high')
      and lower(trim(coalesce(b.route_status, ''))) = 'routed'
      and (coalesce(b.has_sea, false) or coalesce(b.has_inland_water, false))
      then 'HIGH'
    when b._is_maritime_routed_base and (coalesce(b.has_sea, false) or coalesce(b.has_inland_water, false))
      then 'MEDIUM'
    else 'LOW'
  end as route_confidence_score,
  b.route_scenario,
  b.sea_distance_km,
  b.sea_distance_direct_km,
  b.sea_distance_forced_km,
  b.reporter_port,
  b.partner_port,
  b.reporter_basin,
  b.partner_basin,
  b.used_transshipment_hub,
  coalesce(b.has_associated_hub_route, false) as has_associated_hub_route,
  b.hub_port,
  b.hub_iso3,
  b.hub_basin
from base_fact as b