-- Monthly Looker Studio support mart for the Page 4 chokepoint point map.
-- Grain: one row per month_start_date + chokepoint_id.

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
  from `chokepoint-capfractal`.`analytics_marts`.`mart_chokepoint_monthly_stress_detail`
),
exposure_base as (
  select
    month_start_date,
    year_month,
    chokepoint_id,
    chokepoint_name,
    reporter_iso3,
    reporter_country_name,
    chokepoint_trade_value_usd,
    chokepoint_trade_exposure_ratio,
    route_pair_count
  from `chokepoint-capfractal`.`analytics_marts`.`mart_reporter_month_chokepoint_exposure`
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
    count(distinct te.reporter_iso3) as high_medium_exposed_reporter_count
  from `chokepoint-capfractal`.`analytics_marts`.`mart_trade_exposure` as te
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
)

select
  sb.month_start_date,
  sb.year_month,
  format_date('%b %Y', sb.month_start_date) as month_label,
  sb.chokepoint_id,
  sb.chokepoint_name,
  dc.chokepoint_kind,
  dc.longitude,
  dc.latitude,
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
  sb.latest_month_flag
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
left join `chokepoint-capfractal`.`analytics_marts`.`dim_chokepoint` as dc
  on sb.chokepoint_id = dc.chokepoint_id