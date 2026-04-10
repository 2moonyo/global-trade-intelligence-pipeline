
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select monthly_source_coverage_status
from `capfractal`.`analytics_marts`.`mart_executive_monthly_system_snapshot`
where monthly_source_coverage_status is null



  
  
      
    ) dbt_internal_test