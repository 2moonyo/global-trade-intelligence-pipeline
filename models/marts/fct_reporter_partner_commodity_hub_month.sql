with route_candidates as (
  select
    upper(trim(reporter_iso3)) as reporter_iso3,
    upper(trim(partner_iso3)) as partner_iso3,
    main_chokepoint,
    route_status,
    route_confidence,
    routing_decision,
    route_applicability_status,
    route_scenario,
    used_transshipment_hub,
    hub_port,
    hub_iso3,
    row_number() over (
      partition by upper(trim(reporter_iso3)), upper(trim(partner_iso3))
      order by
        case
          when lower(trim(route_scenario)) = 'default_shortest' then 0
          else 1
        end,
        route_scenario
    ) as _rn
  from {{ source('raw', 'dim_trade_routes') }}
),
route_map as (
  select
    reporter_iso3,
    partner_iso3,
    main_chokepoint,
    route_status,
    route_confidence,
    routing_decision,
    route_applicability_status,
    route_scenario,
    used_transshipment_hub,
    hub_port,
    hub_iso3
  from route_candidates
  where _rn = 1
),
hub_applicability as (
  select
    reporter_iso3,
    partner_iso3,
    partner2_iso3,
    sum(coalesce(trade_value_usd, 0)) as partner2_trade_value_usd,
    bool_or(coalesce(has_sea, false)) as has_sea,
    bool_or(coalesce(has_inland_water, false)) as has_inland_water,
    bool_or(coalesce(has_unknown, false)) as has_unknown,
    bool_or(coalesce(has_non_marine, false)) as has_non_marine,
    case
      when bool_or(upper(trim(coalesce(route_applicability_status, ''))) = 'MARITIME_ELIGIBLE') then 'MARITIME_ELIGIBLE'
      when bool_or(upper(trim(coalesce(route_applicability_status, ''))) = 'NON_MARITIME_ONLY') then 'NON_MARITIME_ONLY'
      when bool_or(upper(trim(coalesce(route_applicability_status, ''))) = 'UNKNOWN_MOT') then 'UNKNOWN_MOT'
      when bool_or(upper(trim(coalesce(route_applicability_status, ''))) = 'NO_MOT_DATA') then 'NO_MOT_DATA'
      else null
    end as route_applicability_status
  from {{ ref('stg_route_applicability') }}
  group by 1, 2, 3
),
pair_applicability_totals as (
  select
    reporter_iso3,
    partner_iso3,
    sum(partner2_trade_value_usd) as pair_trade_value_usd,
    count(*) as pair_variant_count
  from hub_applicability
  group by 1, 2
),
hub_allocation as (
  select
    ha.reporter_iso3,
    ha.partner_iso3,
    ha.partner2_iso3,
    case
      when pat.pair_trade_value_usd > 0 then ha.partner2_trade_value_usd / pat.pair_trade_value_usd
      when pat.pair_variant_count > 0 then 1.0 / pat.pair_variant_count
      else 1.0
    end as allocation_share,
    ha.has_sea,
    ha.has_inland_water,
    ha.has_unknown,
    ha.has_non_marine,
    ha.route_applicability_status
  from hub_applicability as ha
  inner join pair_applicability_totals as pat
    on ha.reporter_iso3 = pat.reporter_iso3
   and ha.partner_iso3 = pat.partner_iso3
),
base_pairs as (
  select distinct
    reporter_iso3,
    partner_iso3
  from {{ ref('fct_reporter_partner_commodity_month') }}
),
fallback_allocation as (
  select
    bp.reporter_iso3,
    bp.partner_iso3,
    cast(null as varchar) as partner2_iso3,
    1.0 as allocation_share,
    false as has_sea,
    false as has_inland_water,
    false as has_unknown,
    false as has_non_marine,
    'NO_MOT_DATA' as route_applicability_status
  from base_pairs as bp
  left join pair_applicability_totals as pat
    on bp.reporter_iso3 = pat.reporter_iso3
   and bp.partner_iso3 = pat.partner_iso3
  where pat.reporter_iso3 is null
),
resolved_allocation as (
  select
    reporter_iso3,
    partner_iso3,
    partner2_iso3,
    allocation_share,
    has_sea,
    has_inland_water,
    has_unknown,
    has_non_marine,
    route_applicability_status
  from hub_allocation

  union all

  select
    reporter_iso3,
    partner_iso3,
    partner2_iso3,
    allocation_share,
    has_sea,
    has_inland_water,
    has_unknown,
    has_non_marine,
    route_applicability_status
  from fallback_allocation
),
base_fact as (
  select
    f.reporter_iso3,
    f.partner_iso3,
    ra.partner2_iso3,
    f.cmd_code,
    f.period,
    f.year_month,
    f.ref_year,
    f.trade_flow,
    f.trade_value_usd * ra.allocation_share as trade_value_usd,
    f.net_weight_kg * ra.allocation_share as net_weight_kg,
    f.gross_weight_kg * ra.allocation_share as gross_weight_kg,
    f.qty * ra.allocation_share as qty,
    f.usd_per_kg,
    f.record_count,
    ra.allocation_share,
    rm.main_chokepoint,
    rm.route_status,
    rm.route_confidence,
    rm.routing_decision,
    coalesce(rm.route_applicability_status, ra.route_applicability_status) as route_applicability_status,
    rm.route_scenario,
    rm.used_transshipment_hub,
    rm.hub_port,
    coalesce(rm.hub_iso3, ra.partner2_iso3) as hub_iso3,
    ra.has_sea,
    ra.has_inland_water,
    ra.has_unknown,
    ra.has_non_marine,
    case
      when lower(trim(coalesce(rm.route_status, ''))) = 'routed' then true
      when lower(trim(coalesce(rm.routing_decision, ''))) in ('route_by_observed_sea', 'route_by_inference') then true
      when upper(trim(coalesce(rm.route_applicability_status, ra.route_applicability_status, ''))) = 'MARITIME_ELIGIBLE' then true
      else false
    end as _is_maritime_routed_base
  from {{ ref('fct_reporter_partner_commodity_month') }} as f
  inner join resolved_allocation as ra
    on f.reporter_iso3 = ra.reporter_iso3
   and f.partner_iso3 = ra.partner_iso3
  left join route_map as rm
    on f.reporter_iso3 = rm.reporter_iso3
   and f.partner_iso3 = rm.partner_iso3
)

select
  reporter_iso3,
  partner_iso3,
  partner2_iso3,
  cmd_code,
  period,
  year_month,
  ref_year,
  trade_flow,
  trade_value_usd,
  net_weight_kg,
  gross_weight_kg,
  qty,
  usd_per_kg,
  record_count,
  allocation_share,
  main_chokepoint,
  route_status,
  route_confidence,
  routing_decision,
  route_applicability_status,
  case
    when coalesce(has_non_marine, false) and not coalesce(has_sea, false) and not coalesce(has_inland_water, false)
      then 'NON_MARITIME_ONLY'
    when coalesce(has_unknown, false)
      then 'UNKNOWN_MOT'
    when coalesce(has_sea, false) or coalesce(has_inland_water, false)
      then 'MARITIME_EVIDENCE'
    else 'NO_MOT_DATA'
  end as mot_code_filter_status,
  case
    when coalesce(has_non_marine, false) and not coalesce(has_sea, false) and not coalesce(has_inland_water, false)
      then false
    else _is_maritime_routed_base
  end as is_maritime_routed,
  case
    when coalesce(has_non_marine, false) and not coalesce(has_sea, false) and not coalesce(has_inland_water, false)
      then 'VERY_LOW'
    when coalesce(has_unknown, false)
      then 'LOW'
    when not _is_maritime_routed_base
      then 'LOW'
    when lower(trim(coalesce(route_confidence, ''))) in ('high', 'very_high')
      and lower(trim(coalesce(route_status, ''))) = 'routed'
      and (coalesce(has_sea, false) or coalesce(has_inland_water, false))
      then 'HIGH'
    when _is_maritime_routed_base and (coalesce(has_sea, false) or coalesce(has_inland_water, false))
      then 'MEDIUM'
    else 'LOW'
  end as route_confidence_score,
  case
    when partner2_iso3 is null then 'DIRECT_OR_UNKNOWN'
    else 'HUB_ROUTED'
  end as routing_path_type,
  partner2_iso3 is not null as has_partner2_hub,
  route_scenario,
  used_transshipment_hub,
  hub_port,
  hub_iso3
from base_fact
