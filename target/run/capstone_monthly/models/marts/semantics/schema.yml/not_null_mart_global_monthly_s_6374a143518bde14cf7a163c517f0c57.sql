
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select observed_chokepoint_count
from `capfractal`.`analytics_marts`.`mart_global_monthly_system_stress_summary`
where observed_chokepoint_count is null



  
  
      
    ) dbt_internal_test