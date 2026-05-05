{{ config(materialized='table') }}

with raw_portwatch as (
  select
    {{ cast_string('chokepoint_id') }} as portwatch_source_chokepoint_id,
    {{ canonical_chokepoint_id('chokepoint_name') }} as chokepoint_id,
    {{ canonicalize_chokepoint_name('chokepoint_name') }} as chokepoint_name,
    cast(
      coalesce(
        cast(month_start_date as date),
        {{ month_start_from_year_month('year_month') }}
      ) as date
    ) as month_start_date,
    {{ cast_string('year_month') }} as year_month,
    {{ cast_float('avg_n_total') }} as avg_n_total,
    {{ cast_float('avg_capacity') }} as avg_capacity,
    {{ cast_float('tanker_share') }} as tanker_share,
    {{ cast_float('container_share') }} as container_share,
    {{ cast_float('dry_bulk_share') }} as dry_bulk_share
  from {{ source('raw', 'portwatch_monthly') }}
  where year_month is not null
    and {{ clean_label_text('chokepoint_name') }} is not null
),
chokepoint_bounds as (
  select
    chokepoint_id,
    max(portwatch_source_chokepoint_id) as portwatch_source_chokepoint_id,
    max(chokepoint_name) as chokepoint_name,
    min(month_start_date) as min_month_start,
    max(month_start_date) as max_month_start
  from raw_portwatch
  group by 1
),
calendar as (
  select
    b.chokepoint_id,
    b.portwatch_source_chokepoint_id,
    b.chokepoint_name,
    month_start_date
  from chokepoint_bounds as b,
  {{ month_series('b.min_month_start', 'b.max_month_start') }}
),
scaffolded as (
  select
    c.chokepoint_id,
    coalesce(r.portwatch_source_chokepoint_id, c.portwatch_source_chokepoint_id) as portwatch_source_chokepoint_id,
    coalesce(r.chokepoint_name, c.chokepoint_name) as chokepoint_name,
    c.month_start_date,
    {{ year_month_from_date('c.month_start_date') }} as year_month,
    {{ year_int_from_date('c.month_start_date') }} as year,
    lpad(cast({{ month_int_from_date('c.month_start_date') }} as {{ dbt.type_string() }}), 2, '0') as month,
    r.avg_n_total,
    r.avg_capacity,
    r.tanker_share,
    r.container_share,
    r.dry_bulk_share,
    case when r.year_month is null then 0 else 1 end as has_portwatch_data_flag
  from calendar as c
  left join raw_portwatch as r
    on c.chokepoint_id = r.chokepoint_id
   and c.month_start_date = r.month_start_date
),
signals as (
  select
    *,
    case
      when avg_n_total is null or avg_n_total = 0 then null
      else avg_capacity / avg_n_total
    end as vessel_size_index,
    case
      when chokepoint_name = 'Strait of Hormuz' then tanker_share
      when chokepoint_name in ('Suez Canal', 'Panama Canal') then container_share
      else dry_bulk_share
    end as priority_vessel_share
  from scaffolded
),
windowed as (
  select
    *,
    count(avg_capacity) over expanding_window as expanding_baseline_obs_count,
    avg(avg_capacity) over expanding_window as expanding_mean_capacity,
    stddev_pop(avg_capacity) over expanding_window as expanding_stddev_capacity,
    avg(avg_n_total) over expanding_window as expanding_mean_count,
    stddev_pop(avg_n_total) over expanding_window as expanding_stddev_count,
    avg(vessel_size_index) over expanding_window as expanding_mean_vessel_size,
    stddev_pop(vessel_size_index) over expanding_window as expanding_stddev_vessel_size,
    count(avg_capacity) over rolling_6m_window as rolling_6m_baseline_obs_count,
    avg(avg_capacity) over rolling_6m_window as rolling_6m_mean_capacity,
    stddev_pop(avg_capacity) over rolling_6m_window as rolling_6m_stddev_capacity,
    avg(avg_n_total) over rolling_6m_window as rolling_6m_mean_count,
    stddev_pop(avg_n_total) over rolling_6m_window as rolling_6m_stddev_count,
    avg(vessel_size_index) over rolling_6m_window as rolling_6m_mean_vessel_size,
    stddev_pop(vessel_size_index) over rolling_6m_window as rolling_6m_stddev_vessel_size
  from signals
  window
    expanding_window as (
      partition by chokepoint_id
      order by month_start_date
      rows between unbounded preceding and 1 preceding
    ),
    rolling_6m_window as (
      partition by chokepoint_id
      order by month_start_date
      rows between 6 preceding and 1 preceding
    )
),
scored as (
  select
    chokepoint_id,
    portwatch_source_chokepoint_id,
    chokepoint_name,
    month_start_date,
    year_month,
    year,
    month,
    avg_n_total,
    avg_capacity,
    tanker_share,
    container_share,
    dry_bulk_share,
    priority_vessel_share,
    vessel_size_index,
    has_portwatch_data_flag,
    expanding_baseline_obs_count,
    expanding_mean_capacity,
    expanding_stddev_capacity,
    expanding_mean_count,
    expanding_stddev_count,
    expanding_mean_vessel_size,
    expanding_stddev_vessel_size,
    rolling_6m_baseline_obs_count,
    rolling_6m_mean_capacity,
    rolling_6m_stddev_capacity,
    rolling_6m_mean_count,
    rolling_6m_stddev_count,
    rolling_6m_mean_vessel_size,
    rolling_6m_stddev_vessel_size,
    case
      when has_portwatch_data_flag = 0 then null
      when expanding_baseline_obs_count < 2
        or expanding_stddev_capacity is null
        or expanding_stddev_capacity = 0 then null
      else (avg_capacity - expanding_mean_capacity) / expanding_stddev_capacity
    end as z_score_capacity,
    case
      when has_portwatch_data_flag = 0 then null
      when expanding_baseline_obs_count < 2
        or expanding_stddev_count is null
        or expanding_stddev_count = 0 then null
      else (avg_n_total - expanding_mean_count) / expanding_stddev_count
    end as z_score_count,
    case
      when has_portwatch_data_flag = 0 then null
      when expanding_baseline_obs_count < 2
        or expanding_stddev_vessel_size is null
        or expanding_stddev_vessel_size = 0 then null
      else (vessel_size_index - expanding_mean_vessel_size) / expanding_stddev_vessel_size
    end as z_score_vessel_size,
    case
      when has_portwatch_data_flag = 0 then null
      when rolling_6m_baseline_obs_count < 2
        or rolling_6m_stddev_capacity is null
        or rolling_6m_stddev_capacity = 0 then null
      else (avg_capacity - rolling_6m_mean_capacity) / rolling_6m_stddev_capacity
    end as z_score_capacity_rolling_6m,
    case
      when has_portwatch_data_flag = 0 then null
      when rolling_6m_baseline_obs_count < 2
        or rolling_6m_stddev_count is null
        or rolling_6m_stddev_count = 0 then null
      else (avg_n_total - rolling_6m_mean_count) / rolling_6m_stddev_count
    end as z_score_count_rolling_6m,
    case
      when has_portwatch_data_flag = 0 then null
      when rolling_6m_baseline_obs_count < 2
        or rolling_6m_stddev_vessel_size is null
        or rolling_6m_stddev_vessel_size = 0 then null
      else (vessel_size_index - rolling_6m_mean_vessel_size) / rolling_6m_stddev_vessel_size
    end as z_score_vessel_size_rolling_6m
  from windowed
),
final as (
  select
    chokepoint_id,
    portwatch_source_chokepoint_id,
    chokepoint_name,
    month_start_date,
    year_month,
    year,
    month,
    avg_n_total,
    avg_capacity,
    tanker_share,
    container_share,
    dry_bulk_share,
    priority_vessel_share,
    vessel_size_index,
    has_portwatch_data_flag,
    expanding_baseline_obs_count,
    expanding_mean_capacity as mean_throughput,
    expanding_stddev_capacity as stddev_throughput,
    expanding_mean_count,
    expanding_stddev_count,
    expanding_mean_vessel_size,
    expanding_stddev_vessel_size,
    rolling_6m_baseline_obs_count,
    rolling_6m_mean_capacity,
    rolling_6m_stddev_capacity,
    rolling_6m_mean_count,
    rolling_6m_stddev_count,
    rolling_6m_mean_vessel_size,
    rolling_6m_stddev_vessel_size,
    avg_capacity as throughput,
    z_score_capacity as z_score,
    z_score_capacity,
    z_score_count,
    z_score_vessel_size,
    z_score_capacity_rolling_6m,
    z_score_count_rolling_6m,
    z_score_vessel_size_rolling_6m,
    case
      when z_score_capacity is null or z_score_count is null then null
      else 0.5 * z_score_count + 0.5 * z_score_capacity
    end as stress_index,
    case
      when z_score_capacity is null or z_score_count is null then null
      else (0.5 * z_score_count + 0.5 * z_score_capacity) * (1.0 + 0.5 * coalesce(priority_vessel_share, 0.0))
    end as stress_index_weighted,
    case
      when z_score_capacity_rolling_6m is null or z_score_count_rolling_6m is null then null
      else 0.5 * z_score_count_rolling_6m + 0.5 * z_score_capacity_rolling_6m
    end as stress_index_rolling_6m,
    case
      when z_score_capacity_rolling_6m is null or z_score_count_rolling_6m is null then null
      else (0.5 * z_score_count_rolling_6m + 0.5 * z_score_capacity_rolling_6m) * (1.0 + 0.5 * coalesce(priority_vessel_share, 0.0))
    end as stress_index_weighted_rolling_6m
  from scored
)

select *
from final
where has_portwatch_data_flag = 1
