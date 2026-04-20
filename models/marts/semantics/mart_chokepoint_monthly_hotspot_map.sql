-- Looker Studio support mart for the Page 4 chokepoint point map.
-- Grain: one row per chokepoint_id, using the chokepoint's latest available stress month.

with stress_base as (
  select
    month_start_date,
    year_month,
    chokepoint_id,
    chokepoint_name,
    z_score_historical,
    abs_z_score_historical,
    z_score_historical_capped,
    stress_deviation_score_capped,
    stress_deviation_index_100,
    stress_index,
    stress_index_weighted,
    stress_severity_band,
    latest_month_flag,
    event_active_flag,
    active_event_count
  from {{ ref('mart_chokepoint_monthly_stress_detail') }}
),
eligible_reporters as (
  select
    iso3,
    country_name
  from {{ ref('dim_country') }}
  where is_country_map_eligible
),
exposure_base as (
  select
    e.month_start_date,
    e.year_month,
    e.chokepoint_id,
    e.chokepoint_name,
    er.iso3 as reporter_iso3,
    er.country_name as reporter_country_name,
    e.chokepoint_trade_value_usd,
    e.chokepoint_trade_exposure_ratio,
    e.route_pair_count
  from {{ ref('mart_reporter_month_chokepoint_exposure') }} as e
  inner join eligible_reporters as er
    on {{ canonical_country_iso3('e.reporter_iso3') }} = er.iso3
),
exposure_aggregated as (
  select
    month_start_date,
    year_month,
    chokepoint_id,
    chokepoint_name,
    sum(chokepoint_trade_value_usd) as total_exposed_trade_value_usd,
    count(distinct reporter_iso3) as exposed_reporter_count,
    sum(route_pair_count) as exposed_route_pair_count,
    avg(chokepoint_trade_exposure_ratio) as avg_reporter_exposure_ratio,
    max(chokepoint_trade_exposure_ratio) as max_reporter_exposure_ratio
  from exposure_base
  group by 1, 2, 3, 4
),
high_medium_exposure as (
  select
    te.year_month,
    te.chokepoint_id,
    sum(te.chokepoint_trade_value_usd) as high_medium_exposed_trade_value_usd,
    count(distinct er.iso3) as high_medium_exposed_reporter_count
  from {{ ref('mart_trade_exposure') }} as te
  inner join eligible_reporters as er
    on {{ canonical_country_iso3('te.reporter_iso3') }} = er.iso3
  where te.route_confidence_score in ('HIGH', 'MEDIUM')
  group by 1, 2
),
top_reporter_candidates as (
  select
    month_start_date,
    year_month,
    chokepoint_id,
    reporter_iso3,
    reporter_country_name,
    chokepoint_trade_value_usd,
    row_number() over (
      partition by month_start_date, chokepoint_id
      order by
        chokepoint_trade_value_usd desc,
        reporter_country_name
    ) as exposure_rank_in_chokepoint_month
  from exposure_base
),
top_reporter as (
  select
    month_start_date,
    year_month,
    chokepoint_id,
    reporter_iso3 as top_exposed_reporter_iso3,
    reporter_country_name as top_exposed_reporter_country_name,
    chokepoint_trade_value_usd as top_exposed_reporter_trade_value_usd
  from top_reporter_candidates
  where exposure_rank_in_chokepoint_month = 1
),
hotspot_snapshot as (
  select
    sb.month_start_date,
    sb.year_month,
    format_date('%b %Y', sb.month_start_date) as month_label,
    sb.chokepoint_id,
    sb.chokepoint_name,
    dc.chokepoint_kind,
    dc.longitude,
    dc.latitude,
    dc.geo_point,
    case
      when dc.longitude is not null and dc.latitude is not null then true
      else false
    end as has_map_coordinates_flag,
    sb.z_score_historical,
    sb.abs_z_score_historical,
    sb.z_score_historical_capped,
    sb.stress_deviation_score_capped,
    sb.stress_deviation_index_100,
    sb.stress_index,
    sb.stress_index_weighted,
    sb.stress_severity_band,
    sb.event_active_flag,
    sb.active_event_count,
    coalesce(ea.total_exposed_trade_value_usd, 0) as total_exposed_trade_value_usd,
    coalesce(hm.high_medium_exposed_trade_value_usd, 0) as high_medium_exposed_trade_value_usd,
    coalesce(ea.exposed_reporter_count, 0) as exposed_reporter_count,
    coalesce(hm.high_medium_exposed_reporter_count, 0) as high_medium_exposed_reporter_count,
    coalesce(ea.exposed_route_pair_count, 0) as exposed_route_pair_count,
    ea.avg_reporter_exposure_ratio,
    ea.max_reporter_exposure_ratio,
    tr.top_exposed_reporter_iso3,
    tr.top_exposed_reporter_country_name,
    tr.top_exposed_reporter_trade_value_usd,
    true as latest_month_flag,
    row_number() over (
      partition by sb.chokepoint_id
      order by sb.month_start_date desc, sb.year_month desc
    ) as chokepoint_snapshot_rank
  from stress_base as sb
  left join exposure_aggregated as ea
    on sb.month_start_date = ea.month_start_date
   and sb.chokepoint_id = ea.chokepoint_id
  left join high_medium_exposure as hm
    on sb.year_month = hm.year_month
   and sb.chokepoint_id = hm.chokepoint_id
  left join top_reporter as tr
    on sb.month_start_date = tr.month_start_date
   and sb.chokepoint_id = tr.chokepoint_id
  left join {{ ref('dim_chokepoint') }} as dc
    on sb.chokepoint_id = dc.chokepoint_id
)

select
  month_start_date,
  year_month,
  month_label,
  chokepoint_id,
  chokepoint_name,
  chokepoint_kind,
  longitude,
  latitude,
  geo_point,
  has_map_coordinates_flag,
  z_score_historical,
  abs_z_score_historical,
  z_score_historical_capped,
  stress_deviation_score_capped,
  stress_deviation_index_100,
  stress_index,
  stress_index_weighted,
  stress_severity_band,
  event_active_flag,
  active_event_count,
  total_exposed_trade_value_usd,
  high_medium_exposed_trade_value_usd,
  exposed_reporter_count,
  high_medium_exposed_reporter_count,
  exposed_route_pair_count,
  avg_reporter_exposure_ratio,
  max_reporter_exposure_ratio,
  top_exposed_reporter_iso3,
  top_exposed_reporter_country_name,
  top_exposed_reporter_trade_value_usd,
  latest_month_flag
from hotspot_snapshot
where chokepoint_snapshot_rank = 1
