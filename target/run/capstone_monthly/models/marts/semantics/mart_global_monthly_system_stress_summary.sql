
  
    

    create or replace table `capfractal`.`analytics_marts`.`mart_global_monthly_system_stress_summary`
      
    
    

    
    OPTIONS()
    as (
      -- Monthly Looker Studio scorecard mart for Page 3.
-- Grain: one row per month_start_date.

with base as (
  select
    month_start_date,
    year_month,
    chokepoint_id,
    z_score_historical,
    stress_index,
    stress_index_weighted,
    event_active_flag
  from `capfractal`.`analytics_marts`.`mart_chokepoint_monthly_stress`
),
expected_chokepoints as (
  select count(distinct chokepoint_id) as expected_chokepoint_count
  from base
),
monthly_agg as (
  select
    month_start_date,
    max(year_month) as year_month,
    count(distinct chokepoint_id) as observed_chokepoint_count,
    avg(stress_index) as avg_stress_index,
    avg(stress_index_weighted) as avg_stress_index_weighted,
    avg(abs(z_score_historical)) as avg_abs_z_score_historical,
    max(abs(z_score_historical)) as max_abs_z_score_historical,
    count(
      distinct case
        when z_score_historical is not null
          and abs(z_score_historical) >= 1 then chokepoint_id
      end
    ) as stressed_chokepoint_count,
    count(
      distinct case
        when event_active_flag then chokepoint_id
      end
    ) as event_impacted_chokepoint_count
  from base
  group by 1
),
latest_month as (
  select max(month_start_date) as latest_month_start_date
  from monthly_agg
)

select
  m.month_start_date,
  m.year_month,
  m.observed_chokepoint_count,
  e.expected_chokepoint_count,
  case
    when e.expected_chokepoint_count is null or e.expected_chokepoint_count = 0 then null
    else m.observed_chokepoint_count / e.expected_chokepoint_count
  end as monthly_coverage_ratio,
  case
    when m.observed_chokepoint_count = 0 then 'NO_PORTWATCH_DATA'
    when m.observed_chokepoint_count = e.expected_chokepoint_count then 'FULL_COVERAGE'
    else 'PARTIAL_COVERAGE'
  end as monthly_source_coverage_status,
  m.avg_stress_index,
  m.avg_stress_index_weighted,
  m.avg_abs_z_score_historical,
  m.max_abs_z_score_historical,
  m.stressed_chokepoint_count,
  m.event_impacted_chokepoint_count,
  case
    when m.observed_chokepoint_count = 0 then 'NO_PORTWATCH_DATA'
    when m.max_abs_z_score_historical is null then 'INSUFFICIENT_BASELINE'
    when m.max_abs_z_score_historical >= 2 then 'SEVERE'
    when m.max_abs_z_score_historical >= 1 then 'ELEVATED'
    else 'NORMAL'
  end as system_stress_level,
  case when m.month_start_date = l.latest_month_start_date then true else false end as latest_month_flag
from monthly_agg as m
cross join expected_chokepoints as e
cross join latest_month as l
    );
  