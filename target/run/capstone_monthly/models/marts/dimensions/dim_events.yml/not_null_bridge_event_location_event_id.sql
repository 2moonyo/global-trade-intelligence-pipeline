
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select event_id
from `chokepoint-capfractal`.`analytics_analytics_marts`.`bridge_event_location`
where event_id is null



  
  
      
    ) dbt_internal_test