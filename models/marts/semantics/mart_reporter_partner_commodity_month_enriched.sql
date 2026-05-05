-- Monthly Looker Studio semantic mart for Page 4 partner, commodity, and chokepoint exposure detail.
-- Grain: one row per month_start_date + reporter_iso3 + partner_iso3 + cmd_code + chokepoint_id.

with exposed_trade as (
  select
    {{ canonical_country_iso3('reporter_iso3') }} as reporter_iso3,
    {{ canonical_country_iso3('partner_iso3') }} as partner_iso3,
    cmd_code,
    period,
    year_month,
    trade_flow,
    trade_value_usd,
    net_weight_kg,
    gross_weight_kg,
    record_count,
    {{ canonicalize_chokepoint_name('main_chokepoint') }} as chokepoint_name,
    route_group,
    headline_exposure_group,
    route_confidence_score,
    route_applicability_status,
    used_transshipment_hub,
    {{ canonical_country_iso3('hub_iso3') }} as hub_iso3
  from {{ ref('fct_reporter_partner_commodity_route_month') }}
  where {{ clean_label_text('main_chokepoint') }} is not null
    and coalesce(is_maritime_routed, false)
),
aggregated_trade as (
  -- Route attributes are expected to be stable inside this grain because routing is assigned at pair level.
  select
    reporter_iso3,
    partner_iso3,
    cmd_code,
    period,
    year_month,
    chokepoint_name,
    max(route_group) as route_group,
    max(headline_exposure_group) as headline_exposure_group,
    max(route_confidence_score) as route_confidence_score,
    max(route_applicability_status) as route_applicability_status,
    coalesce(max(used_transshipment_hub), false) as used_transshipment_hub_flag,
    max(hub_iso3) as hub_iso3,
    sum(trade_value_usd) as total_trade_value_usd,
    sum(case when lower(trade_flow) like '%import%' then trade_value_usd else 0 end) as import_trade_value_usd,
    sum(case when lower(trade_flow) like '%export%' then trade_value_usd else 0 end) as export_trade_value_usd,
    sum(net_weight_kg) as total_net_weight_kg,
    sum(gross_weight_kg) as total_gross_weight_kg,
    sum(record_count) as source_row_count
  from exposed_trade
  group by 1, 2, 3, 4, 5, 6
),
with_chokepoint_id as (
  select
    a.*,
    dc.chokepoint_id
  from aggregated_trade as a
  left join {{ ref('dim_chokepoint') }} as dc
    on a.chokepoint_name = dc.chokepoint_name
),
reporter_month_totals as (
  select
    reporter_iso3,
    period,
    year_month,
    month_start_date,
    total_trade_value_usd as reporter_month_trade_value_usd
  from {{ ref('mart_reporter_month_trade_summary') }}
),
reporter_chokepoint_totals as (
  select
    reporter_iso3,
    period,
    year_month,
    chokepoint_id,
    chokepoint_trade_value_usd as reporter_chokepoint_trade_value_usd,
    chokepoint_trade_exposure_ratio
  from {{ ref('mart_reporter_month_chokepoint_exposure') }}
),
stress_context as (
  select
    month_start_date,
    year_month,
    chokepoint_id,
    stress_index,
    stress_index_weighted,
    z_score_historical,
    stress_deviation_score_capped,
    stress_severity_band,
    event_active_flag,
    active_event_count
  from {{ ref('mart_chokepoint_monthly_stress_detail') }}
),
global_bounds as (
  select max(month_start_date) as latest_month_start_date
  from reporter_month_totals
)

select
  a.reporter_iso3,
  rc.country_name as reporter_country_name,
  rc.region as reporter_region,
  rc.subregion as reporter_subregion,
  rc.continent as reporter_continent,
  rc.is_eu as reporter_is_eu,
  rc.is_oecd as reporter_is_oecd,
  rc.is_country_map_eligible as reporter_is_country_map_eligible,
  a.partner_iso3,
  pc.country_name as partner_country_name,
  pc.region as partner_region,
  pc.subregion as partner_subregion,
  pc.continent as partner_continent,
  pc.is_eu as partner_is_eu,
  pc.is_oecd as partner_is_oecd,
  pc.is_country_map_eligible as partner_is_country_map_eligible,
  a.cmd_code,
  co.commodity_name,
  co.commodity_group,
  co.food_flag,
  co.energy_flag,
  co.industrial_flag,
  a.period,
  a.year_month,
  rmt.month_start_date,
  format_date('%b %Y', rmt.month_start_date) as month_label,
  a.chokepoint_id,
  a.chokepoint_name,
  a.route_group,
  a.headline_exposure_group,
  a.route_confidence_score,
  a.route_applicability_status,
  a.used_transshipment_hub_flag,
  a.hub_iso3,
  a.total_trade_value_usd,
  a.import_trade_value_usd,
  a.export_trade_value_usd,
  a.total_net_weight_kg,
  a.total_gross_weight_kg,
  a.source_row_count,
  rmt.reporter_month_trade_value_usd,
  rct.reporter_chokepoint_trade_value_usd,
  rct.chokepoint_trade_exposure_ratio,
  case
    when rmt.reporter_month_trade_value_usd = 0 then null
    else a.total_trade_value_usd / rmt.reporter_month_trade_value_usd
  end as chokepoint_exposed_trade_share_of_reporter_total,
  case
    when rct.reporter_chokepoint_trade_value_usd = 0 then null
    else a.total_trade_value_usd / rct.reporter_chokepoint_trade_value_usd
  end as partner_commodity_trade_share_of_reporter_chokepoint,
  sc.stress_index,
  sc.stress_index_weighted,
  sc.z_score_historical,
  sc.stress_deviation_score_capped,
  sc.stress_severity_band,
  coalesce(sc.event_active_flag, false) as event_active_flag,
  coalesce(sc.active_event_count, 0) as active_event_count,
  case
    when rmt.month_start_date = g.latest_month_start_date then true
    else false
  end as latest_month_flag
from with_chokepoint_id as a
inner join reporter_month_totals as rmt
  on a.reporter_iso3 = rmt.reporter_iso3
 and a.period = rmt.period
 and a.year_month = rmt.year_month
left join reporter_chokepoint_totals as rct
  on a.reporter_iso3 = rct.reporter_iso3
 and a.period = rct.period
 and a.year_month = rct.year_month
 and a.chokepoint_id = rct.chokepoint_id
left join stress_context as sc
  on a.year_month = sc.year_month
 and a.chokepoint_id = sc.chokepoint_id
left join {{ ref('dim_country') }} as rc
  on a.reporter_iso3 = rc.iso3
left join {{ ref('dim_country') }} as pc
  on a.partner_iso3 = pc.iso3
left join {{ ref('dim_commodity') }} as co
  on a.cmd_code = co.cmd_code
cross join global_bounds as g
