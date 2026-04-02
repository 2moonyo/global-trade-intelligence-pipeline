
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select event_id
from `capfractal`.`analytics_analytics_marts`.`bridge_event_region`
where event_id is null



  
  
      
    ) dbt_internal_test