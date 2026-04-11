
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select event_name
from `chokepoint-capfractal`.`analytics_analytics_marts`.`dim_event`
where event_name is null



  
  
      
    ) dbt_internal_test