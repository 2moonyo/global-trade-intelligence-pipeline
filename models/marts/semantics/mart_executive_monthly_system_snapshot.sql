-- Monthly Looker Studio support mart for Page 1 executive system framing.
-- Grain: one row per month_start_date.

with system_summary as (
  select
    month_start_date,
    year_month,
    observed_chokepoint_count,
    expected_chokepoint_count,
    monthly_coverage_ratio,
    monthly_source_coverage_status,
    avg_stress_index,
    avg_stress_index_weighted,
    avg_abs_z_score_historical,
    max_abs_z_score_historical,
    stressed_chokepoint_count,
    event_impacted_chokepoint_count,
    system_stress_level,
    latest_month_flag
  from {{ ref('mart_global_monthly_system_stress_summary') }}
),
hotspot_candidates as (
  -- One hotspot row per month keeps the executive mart compact and avoids map-style duplication.
  select
    month_start_date,
    chokepoint_id,
    chokepoint_name,
    z_score_historical,
    abs(z_score_historical) as abs_z_score_historical,
    stress_index,
    stress_index_weighted,
    event_active_flag,
    active_event_count,
    max_active_event_severity,
    row_number() over (
      partition by month_start_date
      order by
        abs(z_score_historical) desc,
        abs(stress_index_weighted) desc,
        chokepoint_name
    ) as hotspot_rank
  from {{ ref('mart_chokepoint_monthly_stress') }}
  where z_score_historical is not null
),
top_hotspot as (
  select
    month_start_date,
    chokepoint_id as top_stressed_chokepoint_id,
    chokepoint_name as top_stressed_chokepoint_name,
    z_score_historical as top_stressed_chokepoint_z_score_historical,
    abs_z_score_historical as top_stressed_chokepoint_abs_z_score_historical,
    stress_index as top_stressed_chokepoint_stress_index,
    stress_index_weighted as top_stressed_chokepoint_stress_index_weighted,
    event_active_flag as top_stressed_chokepoint_event_active_flag,
    active_event_count as top_stressed_chokepoint_active_event_count,
    max_active_event_severity as top_stressed_chokepoint_max_active_event_severity
  from hotspot_candidates
  where hotspot_rank = 1
),
with_previous as (
  select
    ss.*,
    lag(month_start_date, 1) over (
      order by month_start_date
    ) as previous_month_start_date,
    lag(avg_stress_index, 1) over (
      order by month_start_date
    ) as previous_month_avg_stress_index_raw,
    lag(avg_stress_index_weighted, 1) over (
      order by month_start_date
    ) as previous_month_avg_stress_index_weighted_raw,
    lag(stressed_chokepoint_count, 1) over (
      order by month_start_date
    ) as previous_month_stressed_chokepoint_count_raw,
    lag(event_impacted_chokepoint_count, 1) over (
      order by month_start_date
    ) as previous_month_event_impacted_chokepoint_count_raw
  from system_summary as ss
),
joined as (
  select
    wp.month_start_date,
    wp.year_month,
    format_date('%b %Y', wp.month_start_date) as month_label,
    wp.observed_chokepoint_count,
    wp.expected_chokepoint_count,
    wp.monthly_coverage_ratio,
    wp.monthly_source_coverage_status,
    wp.avg_stress_index,
    wp.avg_stress_index_weighted,
    wp.avg_abs_z_score_historical,
    wp.max_abs_z_score_historical,
    wp.stressed_chokepoint_count,
    wp.event_impacted_chokepoint_count,
    wp.system_stress_level,
    case
      when wp.previous_month_start_date is not null
        and {{ date_add_months('wp.previous_month_start_date', 1) }} = wp.month_start_date then true
      else false
    end as previous_month_available_flag,
    wp.previous_month_avg_stress_index_raw,
    wp.previous_month_avg_stress_index_weighted_raw,
    wp.previous_month_stressed_chokepoint_count_raw,
    wp.previous_month_event_impacted_chokepoint_count_raw,
    th.top_stressed_chokepoint_id,
    th.top_stressed_chokepoint_name,
    th.top_stressed_chokepoint_z_score_historical,
    th.top_stressed_chokepoint_abs_z_score_historical,
    th.top_stressed_chokepoint_stress_index,
    th.top_stressed_chokepoint_stress_index_weighted,
    th.top_stressed_chokepoint_event_active_flag,
    th.top_stressed_chokepoint_active_event_count,
    th.top_stressed_chokepoint_max_active_event_severity,
    wp.latest_month_flag
  from with_previous as wp
  left join top_hotspot as th
    on wp.month_start_date = th.month_start_date
)

select
  month_start_date,
  year_month,
  month_label,
  observed_chokepoint_count,
  expected_chokepoint_count,
  expected_chokepoint_count - observed_chokepoint_count as missing_chokepoint_count,
  monthly_coverage_ratio,
  monthly_source_coverage_status,
  case
    when expected_chokepoint_count - observed_chokepoint_count > 0 then true
    else false
  end as coverage_gap_flag,
  avg_stress_index,
  avg_stress_index_weighted,
  avg_abs_z_score_historical,
  max_abs_z_score_historical,
  stressed_chokepoint_count,
  event_impacted_chokepoint_count,
  system_stress_level,
  previous_month_available_flag,
  case
    when previous_month_available_flag then previous_month_avg_stress_index_raw
    else null
  end as previous_month_avg_stress_index,
  case
    when previous_month_available_flag then previous_month_avg_stress_index_weighted_raw
    else null
  end as previous_month_avg_stress_index_weighted,
  case
    when previous_month_available_flag then previous_month_stressed_chokepoint_count_raw
    else null
  end as previous_month_stressed_chokepoint_count,
  case
    when previous_month_available_flag then previous_month_event_impacted_chokepoint_count_raw
    else null
  end as previous_month_event_impacted_chokepoint_count,
  case
    when previous_month_available_flag
      and previous_month_avg_stress_index_raw is not null then avg_stress_index - previous_month_avg_stress_index_raw
    else null
  end as avg_stress_index_mom_change,
  case
    when previous_month_available_flag
      and previous_month_avg_stress_index_weighted_raw is not null then avg_stress_index_weighted - previous_month_avg_stress_index_weighted_raw
    else null
  end as avg_stress_index_weighted_mom_change,
  case
    when previous_month_available_flag
      and previous_month_stressed_chokepoint_count_raw is not null then stressed_chokepoint_count - previous_month_stressed_chokepoint_count_raw
    else null
  end as stressed_chokepoint_count_mom_change,
  case
    when previous_month_available_flag
      and previous_month_event_impacted_chokepoint_count_raw is not null then event_impacted_chokepoint_count - previous_month_event_impacted_chokepoint_count_raw
    else null
  end as event_impacted_chokepoint_count_mom_change,
  top_stressed_chokepoint_id,
  top_stressed_chokepoint_name,
  top_stressed_chokepoint_z_score_historical,
  top_stressed_chokepoint_abs_z_score_historical,
  top_stressed_chokepoint_stress_index,
  top_stressed_chokepoint_stress_index_weighted,
  top_stressed_chokepoint_event_active_flag,
  top_stressed_chokepoint_active_event_count,
  top_stressed_chokepoint_max_active_event_severity,
  latest_month_flag
from joined
