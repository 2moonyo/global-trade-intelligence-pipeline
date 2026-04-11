
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  with snapshot as (
  select
    month_start_date,
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
  from `chokepoint-capfractal`.`analytics_marts`.`mart_executive_monthly_system_snapshot`
),
system_summary as (
  select
    month_start_date,
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
  from `chokepoint-capfractal`.`analytics_marts`.`mart_global_monthly_system_stress_summary`
)

select
  ss.month_start_date
from system_summary as ss
inner join snapshot as sn
  on ss.month_start_date = sn.month_start_date
where ss.observed_chokepoint_count is distinct from sn.observed_chokepoint_count
   or ss.expected_chokepoint_count is distinct from sn.expected_chokepoint_count
   or ss.monthly_coverage_ratio is distinct from sn.monthly_coverage_ratio
   or ss.monthly_source_coverage_status is distinct from sn.monthly_source_coverage_status
   or ss.avg_stress_index is distinct from sn.avg_stress_index
   or ss.avg_stress_index_weighted is distinct from sn.avg_stress_index_weighted
   or ss.avg_abs_z_score_historical is distinct from sn.avg_abs_z_score_historical
   or ss.max_abs_z_score_historical is distinct from sn.max_abs_z_score_historical
   or ss.stressed_chokepoint_count is distinct from sn.stressed_chokepoint_count
   or ss.event_impacted_chokepoint_count is distinct from sn.event_impacted_chokepoint_count
   or ss.system_stress_level is distinct from sn.system_stress_level
   or ss.latest_month_flag is distinct from sn.latest_month_flag
  
  
      
    ) dbt_internal_test