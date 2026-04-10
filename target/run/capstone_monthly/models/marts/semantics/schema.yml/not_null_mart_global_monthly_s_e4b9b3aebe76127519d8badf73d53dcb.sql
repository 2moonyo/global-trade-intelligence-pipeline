
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select monthly_source_coverage_status
from `capfractal`.`analytics_marts`.`mart_global_monthly_system_stress_summary`
where monthly_source_coverage_status is null



  
  
      
    ) dbt_internal_test