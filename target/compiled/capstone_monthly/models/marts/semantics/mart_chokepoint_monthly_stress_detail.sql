-- Monthly Looker Studio support mart for Page 1 chokepoint detail.
-- Grain: one row per month_start_date + chokepoint_id.

with base as (
  select
    month_start_date,
    year_month,
    format_date('%b %Y', month_start_date) as month_label,
    chokepoint_id,
    chokepoint_name,
    z_score_historical,
    stress_index,
    stress_index_weighted,
    previous_month_available_flag,
    previous_month_stress_index,
    previous_month_stress_index_weighted,
    stress_index_mom_change,
    stress_index_weighted_mom_change,
    latest_month_flag,
    event_active_flag,
    active_event_count
  from `capfractal`.`analytics_marts`.`mart_chokepoint_monthly_stress`
),
with_previous_z_score as (
  select
    b.*,
    lag(z_score_historical, 1) over (
      partition by chokepoint_id
      order by month_start_date
    ) as previous_month_z_score_historical_raw
  from base as b
),
prepared as (
  select
    month_start_date,
    year_month,
    month_label,
    chokepoint_id,
    chokepoint_name,
    z_score_historical,
    abs(z_score_historical) as abs_z_score_historical,
    stress_index,
    stress_index_weighted,
    previous_month_available_flag,
    previous_month_stress_index,
    previous_month_stress_index_weighted,
    case
      when previous_month_available_flag then previous_month_z_score_historical_raw
      else null
    end as previous_month_z_score_historical,
    stress_index_mom_change,
    stress_index_weighted_mom_change,
    case
      when previous_month_available_flag
        and previous_month_z_score_historical_raw is not null then abs(z_score_historical) - abs(previous_month_z_score_historical_raw)
      else null
    end as abs_z_score_historical_mom_change,
    latest_month_flag,
    event_active_flag,
    active_event_count,
    case
      when z_score_historical is null then 'INSUFFICIENT_BASELINE'
      when abs(z_score_historical) >= 2 then 'SEVERE'
      when abs(z_score_historical) >= 1 then 'ELEVATED'
      else 'NORMAL'
    end as stress_hotspot_band,
    case
      when z_score_historical is null then null
      when z_score_historical > 3 then 3
      when z_score_historical < -3 then -3
      else z_score_historical
    end as z_score_historical_capped,
    case
      when z_score_historical is null then null
      else least(abs(z_score_historical), 3)
    end as stress_deviation_score_capped,
    case
      when z_score_historical is null then null
      else 100 * least(abs(z_score_historical) / 3, 1)
    end as stress_deviation_index_100,
    case
      when z_score_historical is null then 'INSUFFICIENT_BASELINE'
      when abs(z_score_historical) >= 3 then 'EXTREME'
      when abs(z_score_historical) >= 2 then 'SEVERE'
      when abs(z_score_historical) >= 1 then 'ELEVATED'
      else 'NORMAL'
    end as stress_severity_band,
    case
      when not previous_month_available_flag then 'NO_PRIOR_MONTH'
      when z_score_historical is null or previous_month_z_score_historical_raw is null then 'INSUFFICIENT_BASELINE'
      when abs(z_score_historical) > abs(previous_month_z_score_historical_raw) then 'MORE_STRESSED'
      when abs(z_score_historical) < abs(previous_month_z_score_historical_raw) then 'LESS_STRESSED'
      else 'UNCHANGED'
    end as stress_direction
  from with_previous_z_score
),
ranked as (
  select
    p.*,
    row_number() over (
      partition by month_start_date
      order by
        abs_z_score_historical desc,
        abs(stress_index_weighted) desc,
        chokepoint_name
    ) as raw_stress_rank_in_month
  from prepared as p
)

select
  month_start_date,
  year_month,
  month_label,
  chokepoint_id,
  chokepoint_name,
  z_score_historical,
  abs_z_score_historical,
  z_score_historical_capped,
  stress_deviation_score_capped,
  stress_deviation_index_100,
  stress_index,
  stress_index_weighted,
  previous_month_available_flag,
  previous_month_stress_index,
  previous_month_stress_index_weighted,
  previous_month_z_score_historical,
  stress_index_mom_change,
  stress_index_weighted_mom_change,
  abs_z_score_historical_mom_change,
  latest_month_flag,
  event_active_flag,
  active_event_count,
  stress_hotspot_band,
  stress_severity_band,
  stress_direction,
  case
    when z_score_historical is null then null
    else raw_stress_rank_in_month
  end as stress_rank_in_month,
  case
    when z_score_historical is null then false
    when raw_stress_rank_in_month <= 5 then true
    else false
  end as top_5_stressed_chokepoint_flag
from ranked