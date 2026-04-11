
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select missing_chokepoint_count
from `chokepoint-capfractal`.`analytics_marts`.`mart_executive_monthly_system_snapshot`
where missing_chokepoint_count is null



  
  
      
    ) dbt_internal_test