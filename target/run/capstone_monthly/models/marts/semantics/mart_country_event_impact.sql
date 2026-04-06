
  
    

    create or replace table `capfractal`.`analytics_marts`.`mart_country_event_impact`
      
    
    

    
    OPTIONS()
    as (
      -- Grain: one row per reporter_country_code + event_id + period.
-- Purpose: reporter-event monthly impact view for before/during/after storytelling with confidence-filtered exposure.

with event_month_windows as (
  -- Use conformed event-month bridge to keep event windows and phase flags consistent.
  select
    bem.event_id,
    bem.month_key as period,
    bem.year_month,
    bem.month_start_date,
    bem.has_active_phase,
    bem.is_event_active,
    bem.is_lead_period,
    bem.is_lag_period,
    bem.severity_weight,
    bem.is_global_event
  from `capfractal`.`analytics_analytics_marts`.`bridge_event_month` as bem
),
event_month_chokepoints as (
  -- Join event-month rows to affected chokepoints for monthly exposure mapping.
  select
    emw.event_id,
    emw.period,
    emw.year_month,
    emw.month_start_date,
    emw.has_active_phase,
    emw.is_event_active,
    emw.is_lead_period,
    emw.is_lag_period,
    emw.severity_weight,
    emw.is_global_event,
    bec.chokepoint_id,
    bec.chokepoint_name
  from event_month_windows as emw
  inner join `capfractal`.`analytics_analytics_marts`.`bridge_event_chokepoint` as bec
    on emw.event_id = bec.event_id
),
reporter_event_exposure as (
  -- Join confidence-segmented exposure to event chokepoint-month rows at the same month and chokepoint.
  select
    emc.event_id,
    te.reporter_iso3 as reporter_country_code,
    emc.period,
    emc.year_month,
    max(emc.month_start_date) as month_start_date,
    max(emc.has_active_phase) as has_active_phase,
    max(emc.is_event_active) as is_event_active,
    max(emc.is_lead_period) as is_lead_period,
    max(emc.is_lag_period) as is_lag_period,
    max(emc.severity_weight) as severity_weight,
    max(emc.is_global_event) as is_global_event,
    sum(case when te.route_confidence_score in ('HIGH', 'MEDIUM') then te.chokepoint_trade_value_usd else 0 end) as event_exposed_trade_value_usd,
    max(te.reporter_month_trade_value_usd) as reporter_trade_value_usd,
    count(distinct case when te.route_confidence_score in ('HIGH', 'MEDIUM') and te.chokepoint_trade_value_usd > 0 then emc.chokepoint_id end) as affected_chokepoint_count_for_reporter
  from event_month_chokepoints as emc
  inner join `capfractal`.`analytics_marts`.`mart_trade_exposure` as te
    on emc.chokepoint_id = te.chokepoint_id
   and emc.year_month = te.year_month
  group by 1, 2, 3, 4
),
event_month_stress as (
  -- Aggregate stress context at event-month grain for interpretability.
  select
    emc.event_id,
    emc.period,
    emc.year_month,
    avg(z.z_score_capacity) as mean_event_window_zscore_capacity,
    max(z.z_score_capacity) as max_event_window_zscore_capacity,
    avg(z.z_score_count) as mean_event_window_zscore_count,
    avg(z.z_score_vessel_size) as mean_event_window_zscore_vessel_size
  from event_month_chokepoints as emc
  left join `capfractal`.`analytics_staging`.`stg_chokepoint_stress_zscore` as z
    on emc.chokepoint_id = z.chokepoint_id
   and emc.year_month = z.year_month
  group by 1, 2, 3
),
event_region_counts as (
  -- Event region counts provide additional scope context beyond chokepoints.
  select
    ber.event_id,
    count(distinct ber.region_id) as affected_region_count
  from `capfractal`.`analytics_analytics_marts`.`bridge_event_region` as ber
  group by 1
),
event_metadata as (
  -- Event dimension supplies business-facing event descriptors.
  select
    de.event_id,
    de.event_name,
    de.event_type,
    de.severity_level,
    de.event_scope_type,
    de.event_start_date,
    de.event_end_date,
    de.description
  from `capfractal`.`analytics_analytics_marts`.`dim_event` as de
),
event_profile as (
  -- Existing event impact mart contributes event-level structural and disruption metrics.
  select
    mei.event_id,
    mei.affected_country_count,
    mei.affected_chokepoint_count,
    mei.mean_baseline_throughput_of_affected_chokepoints,
    mei.mean_throughput_pct_change_vs_baseline
  from `capfractal`.`analytics_marts`.`mart_event_impact` as mei
),
scored_reporter_events as (
  -- Convert exposure scale into dashboard-friendly percentages and phase labels.
  select
    ree.reporter_country_code,
    ree.event_id,
    ree.period,
    ree.year_month,
    ree.month_start_date,
    ree.has_active_phase,
    ree.is_event_active,
    ree.is_lead_period,
    ree.is_lag_period,
    ree.severity_weight,
    ree.is_global_event,
    ree.event_exposed_trade_value_usd,
    ree.reporter_trade_value_usd,
    ree.affected_chokepoint_count_for_reporter,
    case
    when ree.reporter_trade_value_usd is null or ree.reporter_trade_value_usd = 0 then null
    else ree.event_exposed_trade_value_usd / ree.reporter_trade_value_usd
  end * 100 as event_exposure_pct,
    ems.mean_event_window_zscore_capacity,
    ems.max_event_window_zscore_capacity,
    ems.mean_event_window_zscore_count,
    ems.mean_event_window_zscore_vessel_size,
    case
      when ree.is_event_active then 'during'
      when ree.is_lead_period then 'before'
      when ree.is_lag_period then 'after'
      else 'outside_window'
    end as event_phase_label
  from reporter_event_exposure as ree
  left join event_month_stress as ems
    on ree.event_id = ems.event_id
   and ree.period = ems.period
   and ree.year_month = ems.year_month
),
dim_enriched as (
  -- Join reporter country, event metadata, and conformed calendar attributes.
  select
    sre.reporter_country_code,
    country_dim.country_name as reporter_country_name,
    country_dim.region as reporter_region,
    country_dim.subregion as reporter_subregion,
    country_dim.continent as reporter_continent,
    sre.event_id,
    em.event_name,
    em.event_type,
    em.severity_level,
    em.event_scope_type,
    em.event_start_date,
    em.event_end_date,
    em.description as event_description,
    sre.period,
    sre.year_month,
    coalesce(sre.month_start_date, t.month_start_date) as month_start_date,
    t.year,
    t.month,
    sre.has_active_phase,
    sre.is_event_active,
    sre.is_lead_period,
    sre.is_lag_period,
    sre.event_phase_label,
    sre.severity_weight,
    sre.is_global_event,
    sre.event_exposed_trade_value_usd,
    sre.reporter_trade_value_usd,
    sre.event_exposure_pct,
    sre.affected_chokepoint_count_for_reporter,
    coalesce(erc.affected_region_count, 0) as affected_region_count,
    ep.affected_country_count,
    ep.affected_chokepoint_count,
    ep.mean_baseline_throughput_of_affected_chokepoints,
    ep.mean_throughput_pct_change_vs_baseline,
    sre.mean_event_window_zscore_capacity,
    sre.max_event_window_zscore_capacity,
    sre.mean_event_window_zscore_count,
    sre.mean_event_window_zscore_vessel_size
  from scored_reporter_events as sre
  -- Join maps reporter ISO3 to readable country attributes.
  left join `capfractal`.`analytics_marts`.`dim_country` as country_dim
    on sre.reporter_country_code = country_dim.iso3
  -- Join appends event-level descriptive metadata.
  left join event_metadata as em
    on sre.event_id = em.event_id
  -- Join appends event-level profile metrics already computed in marts.
  left join event_profile as ep
    on sre.event_id = ep.event_id
  -- Join adds region scope counts by event.
  left join event_region_counts as erc
    on sre.event_id = erc.event_id
  -- Join standardizes calendar attributes.
  left join `capfractal`.`analytics_marts`.`dim_time` as t
    on sre.period = t.period
),
final as (
  -- Final semantic projection combines raw metrics and labels for policy-facing dashboards.
  select
    de.reporter_country_code,
    de.reporter_country_name,
    de.reporter_region,
    de.reporter_subregion,
    de.reporter_continent,
    de.event_id,
    de.event_name,
    de.event_type,
    de.severity_level,
    de.event_scope_type,
    de.event_start_date,
    de.event_end_date,
    de.event_description,
    de.period as year_month_key,
    de.year_month,
    de.month_start_date,
    de.year,
    format_date('%b', de.month_start_date) as month_name_short,
    format_date('%b %Y', de.month_start_date) as month_label,
    de.has_active_phase,
    de.is_event_active,
    de.is_lead_period,
    de.is_lag_period,
    de.event_phase_label,
    de.severity_weight,
    de.is_global_event,
    de.event_exposed_trade_value_usd,
    de.reporter_trade_value_usd,
    de.event_exposure_pct,
    case
      when de.event_exposed_trade_value_usd is null then null
      when abs(de.event_exposed_trade_value_usd) >= 1000000000000 then concat(format('%.2f', de.event_exposed_trade_value_usd / 1000000000000.0), ' trillion')
      when abs(de.event_exposed_trade_value_usd) >= 1000000000 then concat(format('%.2f', de.event_exposed_trade_value_usd / 1000000000.0), ' billion')
      when abs(de.event_exposed_trade_value_usd) >= 1000000 then concat(format('%.2f', de.event_exposed_trade_value_usd / 1000000.0), ' million')
      when abs(de.event_exposed_trade_value_usd) >= 1000 then concat(format('%.1f', de.event_exposed_trade_value_usd / 1000.0), ' thousand')
      else format('%.0f', de.event_exposed_trade_value_usd)
    end as event_exposed_trade_value_label,
    format('%.1f%%', de.event_exposure_pct) as event_exposure_label,
    de.affected_chokepoint_count_for_reporter,
    de.affected_region_count,
    de.affected_country_count,
    de.affected_chokepoint_count,
    de.mean_baseline_throughput_of_affected_chokepoints,
    de.mean_throughput_pct_change_vs_baseline,
    de.mean_event_window_zscore_capacity,
    de.max_event_window_zscore_capacity,
    de.mean_event_window_zscore_count,
    de.mean_event_window_zscore_vessel_size,
    case
      when de.event_exposure_pct >= 20 then 'very_high'
      when de.event_exposure_pct >= 12 then 'high'
      when de.event_exposure_pct >= 6 then 'moderate'
      when de.event_exposure_pct > 0 then 'low'
      else 'very_low'
    end as dependency_level,
    case
      when de.severity_level in ('critical', 'high') and de.event_exposure_pct >= 15 then 'high'
      when de.event_exposure_pct >= 10 then 'medium'
      else 'low'
    end as risk_level
  from dim_enriched as de
)

select
  reporter_country_code,
  reporter_country_name,
  reporter_region,
  reporter_subregion,
  reporter_continent,
  event_id,
  event_name,
  event_type,
  severity_level,
  event_scope_type,
  event_start_date,
  event_end_date,
  event_description,
  year_month_key,
  year_month,
  month_start_date,
  year,
  month_name_short,
  month_label,
  has_active_phase,
  is_event_active,
  is_lead_period,
  is_lag_period,
  event_phase_label,
  severity_weight,
  is_global_event,
  event_exposed_trade_value_usd,
  reporter_trade_value_usd,
  event_exposure_pct,
  event_exposed_trade_value_label,
  event_exposure_label,
  affected_chokepoint_count_for_reporter,
  affected_region_count,
  affected_country_count,
  affected_chokepoint_count,
  mean_baseline_throughput_of_affected_chokepoints,
  mean_throughput_pct_change_vs_baseline,
  mean_event_window_zscore_capacity,
  max_event_window_zscore_capacity,
  mean_event_window_zscore_count,
  mean_event_window_zscore_vessel_size,
  dependency_level,
  risk_level
from final
    );
  