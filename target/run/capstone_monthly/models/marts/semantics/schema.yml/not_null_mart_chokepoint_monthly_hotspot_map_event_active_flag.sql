
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select event_active_flag
from `capfractal`.`analytics_marts`.`mart_chokepoint_monthly_hotspot_map`
where event_active_flag is null



  
  
      
    ) dbt_internal_test