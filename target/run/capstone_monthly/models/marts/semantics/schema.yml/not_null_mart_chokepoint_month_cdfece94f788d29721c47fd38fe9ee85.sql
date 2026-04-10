
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select active_event_count
from `capfractal`.`analytics_marts`.`mart_chokepoint_monthly_stress_detail`
where active_event_count is null



  
  
      
    ) dbt_internal_test