
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select month_key
from `capfractal`.`analytics_analytics_marts`.`bridge_event_month`
where month_key is null



  
  
      
    ) dbt_internal_test