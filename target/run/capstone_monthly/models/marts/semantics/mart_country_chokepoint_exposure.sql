
  
    

    create or replace table `capfractal`.`analytics_marts`.`mart_country_chokepoint_exposure`
      
    
    

    
    OPTIONS()
    as (
      -- Grain: one row per reporter_country_code + chokepoint_id + period.
-- Purpose: business-facing chokepoint dependency view with confidence-aware exposure and context signals.

with exposure_by_confidence as (
  -- Aggregate confidence tiers so users can compare HIGH versus MEDIUM evidence in one row.
  select
    te.reporter_iso3 as reporter_country_code,
    te.chokepoint_id,
    te.chokepoint_name,
    te.period,
    te.year_month,
    sum(case when te.route_confidence_score = 'HIGH' then te.chokepoint_trade_value_usd else 0 end) as high_confidence_exposed_trade_value_usd,
    sum(case when te.route_confidence_score = 'MEDIUM' then te.chokepoint_trade_value_usd else 0 end) as medium_confidence_exposed_trade_value_usd,
    sum(case when te.route_confidence_score in ('HIGH', 'MEDIUM') then te.chokepoint_trade_value_usd else 0 end) as exposed_trade_value_usd,
    max(te.reporter_month_trade_value_usd) as reporter_trade_value_usd,
    max(te.route_pair_count) as route_pair_count
  from `capfractal`.`analytics_marts`.`mart_trade_exposure` as te
  group by 1, 2, 3, 4, 5
),
exposure_metrics as (
  -- Percentage metrics are calculated once here to keep downstream logic readable.
  select
    ebc.reporter_country_code,
    ebc.chokepoint_id,
    ebc.chokepoint_name,
    ebc.period,
    ebc.year_month,
    ebc.high_confidence_exposed_trade_value_usd,
    ebc.medium_confidence_exposed_trade_value_usd,
    ebc.exposed_trade_value_usd,
    ebc.reporter_trade_value_usd,
    ebc.route_pair_count,
    case
    when ebc.reporter_trade_value_usd is null or ebc.reporter_trade_value_usd = 0 then null
    else ebc.exposed_trade_value_usd / ebc.reporter_trade_value_usd
  end * 100 as chokepoint_exposure_pct,
    case
    when ebc.exposed_trade_value_usd is null or ebc.exposed_trade_value_usd = 0 then null
    else ebc.high_confidence_exposed_trade_value_usd / ebc.exposed_trade_value_usd
  end * 100 as high_confidence_share_pct,
    case
    when ebc.exposed_trade_value_usd is null or ebc.exposed_trade_value_usd = 0 then null
    else ebc.medium_confidence_exposed_trade_value_usd / ebc.exposed_trade_value_usd
  end * 100 as medium_confidence_share_pct
  from exposure_by_confidence as ebc
),
context_signals as (
  -- Bring stress and event context so exposure rows can explain risk, not only scale.
  select
    rmce.reporter_iso3 as reporter_country_code,
    rmce.chokepoint_id,
    rmce.period,
    rmce.year_month,
    rmce.stress_index,
    rmce.stress_index_weighted,
    rmce.stress_index_rolling_6m,
    rmce.stress_index_weighted_rolling_6m,
    rmce.active_event_count,
    rmce.max_active_event_severity,
    rmce.avg_active_event_severity
  from `capfractal`.`analytics_marts`.`mart_reporter_month_chokepoint_exposure` as rmce
),
ranked_exposure as (
  -- Rank supports top chokepoint charts for each reporter and month.
  select
    em.reporter_country_code,
    em.chokepoint_id,
    em.chokepoint_name,
    em.period,
    em.year_month,
    em.high_confidence_exposed_trade_value_usd,
    em.medium_confidence_exposed_trade_value_usd,
    em.exposed_trade_value_usd,
    em.reporter_trade_value_usd,
    em.route_pair_count,
    em.chokepoint_exposure_pct,
    em.high_confidence_share_pct,
    em.medium_confidence_share_pct,
    dense_rank() over (
      partition by em.reporter_country_code, em.year_month
      order by em.exposed_trade_value_usd desc, em.chokepoint_id
    ) as chokepoint_rank_by_exposure
  from exposure_metrics as em
),
scored_exposure as (
  -- Dependency and risk levels keep category labels consistent for non-technical users.
  select
    re.reporter_country_code,
    re.chokepoint_id,
    re.chokepoint_name,
    re.period,
    re.year_month,
    re.high_confidence_exposed_trade_value_usd,
    re.medium_confidence_exposed_trade_value_usd,
    re.exposed_trade_value_usd,
    re.reporter_trade_value_usd,
    re.route_pair_count,
    re.chokepoint_exposure_pct,
    re.high_confidence_share_pct,
    re.medium_confidence_share_pct,
    re.chokepoint_rank_by_exposure,
    cs.stress_index,
    cs.stress_index_weighted,
    cs.stress_index_rolling_6m,
    cs.stress_index_weighted_rolling_6m,
    coalesce(cs.active_event_count, 0) as active_event_count,
    cs.max_active_event_severity,
    cs.avg_active_event_severity,
    case
      when re.chokepoint_exposure_pct >= 35 then 'very_high'
      when re.chokepoint_exposure_pct >= 20 then 'high'
      when re.chokepoint_exposure_pct >= 10 then 'moderate'
      when re.chokepoint_exposure_pct >= 5 then 'low'
      else 'very_low'
    end as dependency_level,
    case
      when re.chokepoint_exposure_pct >= 25 and coalesce(cs.active_event_count, 0) > 0 then 'high'
      when re.chokepoint_exposure_pct >= 20 and coalesce(cs.stress_index_weighted_rolling_6m, 0) >= 1.5 then 'high'
      when re.chokepoint_exposure_pct >= 10 then 'medium'
      else 'low'
    end as risk_level
  from ranked_exposure as re
  -- Join keeps the same reporter-chokepoint-month grain and adds stress/event context columns.
  left join context_signals as cs
    on re.reporter_country_code = cs.reporter_country_code
   and re.chokepoint_id = cs.chokepoint_id
   and re.period = cs.period
   and re.year_month = cs.year_month
),
dim_enriched as (
  -- Join attaches conformed country and date fields for consistent Looker filters.
  select
    se.reporter_country_code,
    country_dim.country_name as reporter_country_name,
    country_dim.region as reporter_region,
    country_dim.subregion as reporter_subregion,
    country_dim.continent as reporter_continent,
    se.chokepoint_id,
    chokepoint_dim.chokepoint_name,
    chokepoint_dim.chokepoint_kind,
    chokepoint_dim.longitude as chokepoint_longitude,
    chokepoint_dim.latitude as chokepoint_latitude,
    chokepoint_dim.zone_of_influence_radius_m,
    chokepoint_dim.chokepoint_point_geog,
    chokepoint_dim.zone_of_influence_geog,
    se.period,
    se.year_month,
    t.month_start_date,
    t.year,
    t.month,
    se.high_confidence_exposed_trade_value_usd,
    se.medium_confidence_exposed_trade_value_usd,
    se.exposed_trade_value_usd,
    se.reporter_trade_value_usd,
    se.route_pair_count,
    se.chokepoint_exposure_pct,
    se.high_confidence_share_pct,
    se.medium_confidence_share_pct,
    se.chokepoint_rank_by_exposure,
    se.stress_index,
    se.stress_index_weighted,
    se.stress_index_rolling_6m,
    se.stress_index_weighted_rolling_6m,
    se.active_event_count,
    se.max_active_event_severity,
    se.avg_active_event_severity,
    se.dependency_level,
    se.risk_level
  from scored_exposure as se
  -- Join maps reporter ISO3 into readable country attributes.
  left join `capfractal`.`analytics_marts`.`dim_country` as country_dim
    on se.reporter_country_code = country_dim.iso3
  -- Join maps chokepoint_id to canonical chokepoint naming.
  left join `capfractal`.`analytics_marts`.`dim_chokepoint` as chokepoint_dim
    on se.chokepoint_id = chokepoint_dim.chokepoint_id
  -- Join standardizes month fields for reusable date controls.
  left join `capfractal`.`analytics_marts`.`dim_time` as t
    on se.period = t.period
),
final as (
  -- Final semantic projection keeps raw metrics and display labels in one model.
  select
    de.reporter_country_code,
    de.reporter_country_name,
    de.reporter_region,
    de.reporter_subregion,
    de.reporter_continent,
    de.chokepoint_id,
    de.chokepoint_name,
    de.chokepoint_kind,
    de.chokepoint_longitude,
    de.chokepoint_latitude,
    de.zone_of_influence_radius_m,
    de.chokepoint_point_geog,
    de.zone_of_influence_geog,
    de.period as year_month_key,
    de.year_month,
    de.month_start_date,
    de.year,
    format_date('%b', de.month_start_date) as month_name_short,
    format_date('%b %Y', de.month_start_date) as month_label,
    de.high_confidence_exposed_trade_value_usd,
    de.medium_confidence_exposed_trade_value_usd,
    de.exposed_trade_value_usd,
    de.reporter_trade_value_usd,
    de.route_pair_count,
    de.exposed_trade_value_usd / 1000000000.0 as exposed_trade_value_billion,
    case
      when de.exposed_trade_value_usd is null then null
      when abs(de.exposed_trade_value_usd) >= 1000000000000 then concat(format('%.2f', de.exposed_trade_value_usd / 1000000000000.0), ' trillion')
      when abs(de.exposed_trade_value_usd) >= 1000000000 then concat(format('%.2f', de.exposed_trade_value_usd / 1000000000.0), ' billion')
      when abs(de.exposed_trade_value_usd) >= 1000000 then concat(format('%.2f', de.exposed_trade_value_usd / 1000000.0), ' million')
      when abs(de.exposed_trade_value_usd) >= 1000 then concat(format('%.1f', de.exposed_trade_value_usd / 1000.0), ' thousand')
      else format('%.0f', de.exposed_trade_value_usd)
    end as exposed_trade_value_label,
    de.chokepoint_exposure_pct,
    format('%.1f%%', de.chokepoint_exposure_pct) as chokepoint_exposure_label,
    de.high_confidence_share_pct,
    de.medium_confidence_share_pct,
    de.chokepoint_rank_by_exposure,
    de.stress_index,
    de.stress_index_weighted,
    de.stress_index_rolling_6m,
    de.stress_index_weighted_rolling_6m,
    de.active_event_count,
    de.max_active_event_severity,
    de.avg_active_event_severity,
    de.dependency_level,
    de.risk_level
  from dim_enriched as de
)

select
  reporter_country_code,
  reporter_country_name,
  reporter_region,
  reporter_subregion,
  reporter_continent,
  chokepoint_id,
  chokepoint_name,
  chokepoint_kind,
  chokepoint_longitude,
  chokepoint_latitude,
  zone_of_influence_radius_m,
  chokepoint_point_geog,
  zone_of_influence_geog,
  year_month_key,
  year_month,
  month_start_date,
  year,
  month_name_short,
  month_label,
  high_confidence_exposed_trade_value_usd,
  medium_confidence_exposed_trade_value_usd,
  exposed_trade_value_usd,
  reporter_trade_value_usd,
  route_pair_count,
  exposed_trade_value_billion,
  exposed_trade_value_label,
  chokepoint_exposure_pct,
  chokepoint_exposure_label,
  high_confidence_share_pct,
  medium_confidence_share_pct,
  chokepoint_rank_by_exposure,
  stress_index,
  stress_index_weighted,
  stress_index_rolling_6m,
  stress_index_weighted_rolling_6m,
  active_event_count,
  max_active_event_severity,
  avg_active_event_severity,
  dependency_level,
  risk_level
from final
    );
  