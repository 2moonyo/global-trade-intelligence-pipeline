
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  -- Fails when monthly coverage falls outside the valid 0 to 1 range.
select *
from `chokepoint-capfractal`.`analytics_marts`.`mart_global_monthly_system_stress_summary`
where monthly_coverage_ratio < 0
   or monthly_coverage_ratio > 1
  
  
      
    ) dbt_internal_test