-- Daily Looker Studio semantic mart for Page 2.
-- Grain: one row per date_day + chokepoint_id.

with base as (
  select
    date_day,
    month_start_date,
    year_month,
    chokepoint_id,
    chokepoint_name,
    portwatch_source_chokepoint_id,
    has_portwatch_daily_data_flag,
    capacity as throughput_metric,
    n_total as vessel_count_metric,
    vessel_size_index,
    tanker_share,
    container_share,
    dry_bulk_share,
    priority_vessel_share
  from `capfractal`.`analytics_staging`.`stg_portwatch_daily`
),
freshness as (
  select
    chokepoint_id,
    max(case when has_portwatch_daily_data_flag = 1 then date_day end) as latest_observed_date_day
  from base
  group by 1
),
global_bounds as (
  select max(date_day) as latest_date_day
  from base
),
windowed as (
  select
    b.*,
    count(throughput_metric) over trailing_7d as observed_days_in_7d_window,
    count(throughput_metric) over trailing_30d as observed_days_in_30d_window,
    avg(throughput_metric) over trailing_7d as rolling_mean_7d,
    avg(throughput_metric) over trailing_30d as rolling_mean_30d,
    stddev_pop(throughput_metric) over trailing_30d as rolling_stddev_30d,
    avg(vessel_count_metric) over trailing_7d as vessel_count_rolling_mean_7d,
    avg(vessel_count_metric) over trailing_30d as vessel_count_rolling_mean_30d,
    stddev_pop(vessel_count_metric) over trailing_30d as vessel_count_rolling_stddev_30d,
    lag(throughput_metric, 1) over (
      partition by chokepoint_id
      order by date_day
    ) as throughput_metric_1d_ago,
    lag(throughput_metric, 7) over (
      partition by chokepoint_id
      order by date_day
    ) as throughput_metric_7d_ago
  from base as b
  window
    trailing_7d as (
      partition by chokepoint_id
      order by date_day
      rows between 7 preceding and 1 preceding
    ),
    trailing_30d as (
      partition by chokepoint_id
      order by date_day
      rows between 30 preceding and 1 preceding
    )
),
scored as (
  select
    w.date_day,
    w.month_start_date,
    w.year_month,
    w.chokepoint_id,
    w.chokepoint_name,
    w.portwatch_source_chokepoint_id,
    w.has_portwatch_daily_data_flag,
    w.throughput_metric,
    w.vessel_count_metric,
    w.vessel_size_index,
    w.tanker_share,
    w.container_share,
    w.dry_bulk_share,
    w.priority_vessel_share,
    w.observed_days_in_7d_window,
    w.observed_days_in_30d_window,
    w.rolling_mean_7d,
    w.rolling_mean_30d,
    w.rolling_stddev_30d,
    case
      when w.has_portwatch_daily_data_flag = 0 then null
      when w.observed_days_in_30d_window < 2
        or w.rolling_stddev_30d is null
        or w.rolling_stddev_30d = 0 then null
      else (w.throughput_metric - w.rolling_mean_30d) / w.rolling_stddev_30d
    end as z_score_rolling_30d,
    w.vessel_count_rolling_mean_7d,
    w.vessel_count_rolling_mean_30d,
    w.vessel_count_rolling_stddev_30d,
    case
      when w.has_portwatch_daily_data_flag = 0 then null
      when w.observed_days_in_30d_window < 2
        or w.vessel_count_rolling_stddev_30d is null
        or w.vessel_count_rolling_stddev_30d = 0 then null
      else (w.vessel_count_metric - w.vessel_count_rolling_mean_30d) / w.vessel_count_rolling_stddev_30d
    end as vessel_count_z_score_rolling_30d,
    case
      when w.has_portwatch_daily_data_flag = 0 then null
      when w.throughput_metric_1d_ago is null or w.throughput_metric_1d_ago = 0 then null
      else (w.throughput_metric - w.throughput_metric_1d_ago) / w.throughput_metric_1d_ago
    end as pct_change_1d,
    case
      when w.has_portwatch_daily_data_flag = 0 then null
      when w.throughput_metric_7d_ago is null or w.throughput_metric_7d_ago = 0 then null
      else (w.throughput_metric - w.throughput_metric_7d_ago) / w.throughput_metric_7d_ago
    end as pct_change_7d
  from windowed as w
),
final as (
  select
    s.date_day,
    s.month_start_date,
    s.year_month,
    s.chokepoint_id,
    s.chokepoint_name,
    s.portwatch_source_chokepoint_id,
    s.has_portwatch_daily_data_flag,
    s.throughput_metric,
    s.vessel_count_metric,
    s.vessel_size_index,
    s.tanker_share,
    s.container_share,
    s.dry_bulk_share,
    s.priority_vessel_share,
    s.observed_days_in_7d_window,
    s.observed_days_in_30d_window,
    s.rolling_mean_7d,
    s.rolling_mean_30d,
    s.rolling_stddev_30d,
    s.z_score_rolling_30d,
    s.vessel_count_rolling_mean_7d,
    s.vessel_count_rolling_mean_30d,
    s.vessel_count_rolling_stddev_30d,
    s.vessel_count_z_score_rolling_30d,
    case
      when s.z_score_rolling_30d is null or s.vessel_count_z_score_rolling_30d is null then null
      else 0.5 * s.z_score_rolling_30d + 0.5 * s.vessel_count_z_score_rolling_30d
    end as signal_index_rolling_30d,
    s.pct_change_1d,
    s.pct_change_7d,
    f.latest_observed_date_day,
    
    date_diff(cast(g.latest_date_day as date), cast(f.latest_observed_date_day as date), day)
   as days_since_latest_observation,
    case when s.date_day = g.latest_date_day then true else false end as latest_day_flag,
    case when s.date_day = f.latest_observed_date_day then true else false end as latest_observed_day_flag,
    case
      when s.has_portwatch_daily_data_flag = 0 then 'NO_DATA'
      when s.pct_change_1d is null then 'NO_PRIOR_DAY'
      when s.pct_change_1d > 0 then 'UP'
      when s.pct_change_1d < 0 then 'DOWN'
      else 'FLAT'
    end as direction_of_change,
    -- Z-score bands are intentionally simple and are based on absolute deviation
    -- from the prior 30-day baseline.
    case
      when s.has_portwatch_daily_data_flag = 0 then 'NO_DATA'
      when s.z_score_rolling_30d is null then 'INSUFFICIENT_BASELINE'
      when abs(s.z_score_rolling_30d) >= 2 then 'SEVERE'
      when abs(s.z_score_rolling_30d) >= 1 then 'ELEVATED'
      else 'NORMAL'
    end as alert_band
  from scored as s
  left join freshness as f
    on s.chokepoint_id = f.chokepoint_id
  cross join global_bounds as g
)

select *
from final