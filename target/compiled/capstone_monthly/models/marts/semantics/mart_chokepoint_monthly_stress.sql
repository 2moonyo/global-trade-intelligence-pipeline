-- Monthly Looker Studio semantic mart for Page 3.
-- Grain: one row per month_start_date + chokepoint_id.

with base as (
  select
    month_start_date,
    year_month,
    chokepoint_id,
    chokepoint_name,
    portwatch_source_chokepoint_id,
    avg_capacity as throughput_metric,
    avg_n_total as vessel_count_metric,
    priority_vessel_share,
    vessel_size_index,
    expanding_baseline_obs_count as historical_baseline_observation_count,
    mean_throughput as baseline_mean_throughput_historical,
    stddev_throughput as baseline_stddev_throughput_historical,
    expanding_mean_count as baseline_mean_vessel_count_historical,
    expanding_stddev_count as baseline_stddev_vessel_count_historical,
    expanding_mean_vessel_size as baseline_mean_vessel_size_historical,
    expanding_stddev_vessel_size as baseline_stddev_vessel_size_historical,
    z_score as z_score_historical,
    z_score_count as z_score_count_historical,
    z_score_vessel_size as z_score_vessel_size_historical,
    stress_index,
    stress_index_weighted,
    rolling_6m_baseline_obs_count as rolling_6m_baseline_observation_count,
    rolling_6m_mean_capacity as baseline_mean_throughput_rolling_6m,
    rolling_6m_stddev_capacity as baseline_stddev_throughput_rolling_6m,
    rolling_6m_mean_count as baseline_mean_vessel_count_rolling_6m,
    rolling_6m_stddev_count as baseline_stddev_vessel_count_rolling_6m,
    rolling_6m_mean_vessel_size as baseline_mean_vessel_size_rolling_6m,
    rolling_6m_stddev_vessel_size as baseline_stddev_vessel_size_rolling_6m,
    z_score_capacity_rolling_6m as z_score_rolling_6m,
    z_score_count_rolling_6m,
    z_score_vessel_size_rolling_6m,
    stress_index_rolling_6m,
    stress_index_weighted_rolling_6m
  from `capfractal`.`analytics_staging`.`stg_portwatch_stress_metrics`
),
active_event_overlay as (
  -- Aggregate event rows before the join so monthly stress stays at month + chokepoint grain.
  select
    year_month,
    
    to_hex(md5(cast(lower(trim(chokepoint_name)) as string)))
   as chokepoint_id,
    count(distinct case when is_event_active then event_id end) as active_event_count,
    max(case when is_event_active then severity_weight end) as max_active_event_severity,
    avg(case when is_event_active then severity_weight end) as avg_active_event_severity
  from `capfractal`.`analytics_staging`.`stg_chokepoint_bridge`
  group by 1, 2
),
freshness as (
  select
    chokepoint_id,
    max(month_start_date) as latest_observed_month_start_date
  from base
  group by 1
),
global_bounds as (
  select max(month_start_date) as latest_month_start_date
  from base
),
with_previous as (
  select
    b.*,
    lag(month_start_date, 1) over (
      partition by chokepoint_id
      order by month_start_date
    ) as previous_month_start_date,
    lag(throughput_metric, 1) over (
      partition by chokepoint_id
      order by month_start_date
    ) as previous_month_throughput_metric_raw,
    lag(vessel_count_metric, 1) over (
      partition by chokepoint_id
      order by month_start_date
    ) as previous_month_vessel_count_metric_raw,
    lag(stress_index, 1) over (
      partition by chokepoint_id
      order by month_start_date
    ) as previous_month_stress_index_raw,
    lag(stress_index_weighted, 1) over (
      partition by chokepoint_id
      order by month_start_date
    ) as previous_month_stress_index_weighted_raw
  from base as b
),
joined as (
  select
    w.month_start_date,
    w.year_month,
    w.chokepoint_id,
    w.chokepoint_name,
    w.portwatch_source_chokepoint_id,
    w.throughput_metric,
    w.vessel_count_metric,
    w.priority_vessel_share,
    w.vessel_size_index,
    w.historical_baseline_observation_count,
    w.baseline_mean_throughput_historical,
    w.baseline_stddev_throughput_historical,
    w.baseline_mean_vessel_count_historical,
    w.baseline_stddev_vessel_count_historical,
    w.baseline_mean_vessel_size_historical,
    w.baseline_stddev_vessel_size_historical,
    w.z_score_historical,
    w.z_score_count_historical,
    w.z_score_vessel_size_historical,
    w.stress_index,
    w.stress_index_weighted,
    w.rolling_6m_baseline_observation_count,
    w.baseline_mean_throughput_rolling_6m,
    w.baseline_stddev_throughput_rolling_6m,
    w.baseline_mean_vessel_count_rolling_6m,
    w.baseline_stddev_vessel_count_rolling_6m,
    w.baseline_mean_vessel_size_rolling_6m,
    w.baseline_stddev_vessel_size_rolling_6m,
    w.z_score_rolling_6m,
    w.z_score_count_rolling_6m,
    w.z_score_vessel_size_rolling_6m,
    w.stress_index_rolling_6m,
    w.stress_index_weighted_rolling_6m,
    case
      when w.previous_month_start_date is not null
        and 
    date_add(cast(w.previous_month_start_date as date), interval 1 month)
   = w.month_start_date then true
      else false
    end as previous_month_available_flag,
    w.previous_month_throughput_metric_raw,
    w.previous_month_vessel_count_metric_raw,
    w.previous_month_stress_index_raw,
    w.previous_month_stress_index_weighted_raw,
    f.latest_observed_month_start_date,
    cast(
      (
        extract(year from g.latest_month_start_date) - extract(year from f.latest_observed_month_start_date)
      ) * 12
      + (
        extract(month from g.latest_month_start_date) - extract(month from f.latest_observed_month_start_date)
      ) as INT64
    ) as months_since_latest_observation,
    case when w.month_start_date = g.latest_month_start_date then true else false end as latest_month_flag,
    case when w.month_start_date = f.latest_observed_month_start_date then true else false end as latest_observed_month_flag,
    coalesce(e.active_event_count, 0) as active_event_count,
    case when coalesce(e.active_event_count, 0) > 0 then true else false end as event_active_flag,
    e.max_active_event_severity,
    e.avg_active_event_severity
  from with_previous as w
  left join active_event_overlay as e
    on w.year_month = e.year_month
   and w.chokepoint_id = e.chokepoint_id
  left join freshness as f
    on w.chokepoint_id = f.chokepoint_id
  cross join global_bounds as g
)

select
  month_start_date,
  year_month,
  chokepoint_id,
  chokepoint_name,
  portwatch_source_chokepoint_id,
  throughput_metric,
  vessel_count_metric,
  priority_vessel_share,
  vessel_size_index,
  historical_baseline_observation_count,
  baseline_mean_throughput_historical,
  baseline_stddev_throughput_historical,
  baseline_mean_vessel_count_historical,
  baseline_stddev_vessel_count_historical,
  baseline_mean_vessel_size_historical,
  baseline_stddev_vessel_size_historical,
  z_score_historical,
  z_score_count_historical,
  z_score_vessel_size_historical,
  stress_index,
  stress_index_weighted,
  rolling_6m_baseline_observation_count,
  baseline_mean_throughput_rolling_6m,
  baseline_stddev_throughput_rolling_6m,
  baseline_mean_vessel_count_rolling_6m,
  baseline_stddev_vessel_count_rolling_6m,
  baseline_mean_vessel_size_rolling_6m,
  baseline_stddev_vessel_size_rolling_6m,
  z_score_rolling_6m,
  z_score_count_rolling_6m,
  z_score_vessel_size_rolling_6m,
  stress_index_rolling_6m,
  stress_index_weighted_rolling_6m,
  previous_month_available_flag,
  case
    when previous_month_available_flag then previous_month_throughput_metric_raw
    else null
  end as previous_month_throughput_metric,
  case
    when previous_month_available_flag then previous_month_vessel_count_metric_raw
    else null
  end as previous_month_vessel_count_metric,
  case
    when previous_month_available_flag then previous_month_stress_index_raw
    else null
  end as previous_month_stress_index,
  case
    when previous_month_available_flag then previous_month_stress_index_weighted_raw
    else null
  end as previous_month_stress_index_weighted,
  case
    when previous_month_available_flag
      and previous_month_throughput_metric_raw is not null then throughput_metric - previous_month_throughput_metric_raw
    else null
  end as throughput_mom_change,
  case
    when previous_month_available_flag
      and previous_month_throughput_metric_raw is not null
      and previous_month_throughput_metric_raw != 0 then (throughput_metric - previous_month_throughput_metric_raw) / previous_month_throughput_metric_raw
    else null
  end as throughput_mom_change_pct,
  case
    when previous_month_available_flag
      and previous_month_vessel_count_metric_raw is not null then vessel_count_metric - previous_month_vessel_count_metric_raw
    else null
  end as vessel_count_mom_change,
  case
    when previous_month_available_flag
      and previous_month_vessel_count_metric_raw is not null
      and previous_month_vessel_count_metric_raw != 0 then (vessel_count_metric - previous_month_vessel_count_metric_raw) / previous_month_vessel_count_metric_raw
    else null
  end as vessel_count_mom_change_pct,
  case
    when previous_month_available_flag
      and previous_month_stress_index_raw is not null then stress_index - previous_month_stress_index_raw
    else null
  end as stress_index_mom_change,
  case
    when previous_month_available_flag
      and previous_month_stress_index_weighted_raw is not null then stress_index_weighted - previous_month_stress_index_weighted_raw
    else null
  end as stress_index_weighted_mom_change,
  latest_observed_month_start_date,
  months_since_latest_observation,
  latest_month_flag,
  latest_observed_month_flag,
  active_event_count,
  event_active_flag,
  max_active_event_severity,
  avg_active_event_severity
from joined