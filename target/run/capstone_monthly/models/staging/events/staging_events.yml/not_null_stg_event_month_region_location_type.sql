
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select location_type
from `capfractal`.`analytics_analytics_staging`.`stg_event_month_region`
where location_type is null



  
  
      
    ) dbt_internal_test