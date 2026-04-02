
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select region_name
from `capfractal`.`analytics_analytics_staging`.`stg_event_month_region`
where region_name is null



  
  
      
    ) dbt_internal_test