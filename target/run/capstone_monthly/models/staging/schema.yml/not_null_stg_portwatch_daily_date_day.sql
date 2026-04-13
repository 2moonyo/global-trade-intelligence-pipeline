
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select date_day
from `fullcap-10111`.`analytics_staging`.`stg_portwatch_daily`
where date_day is null



  
  
      
    ) dbt_internal_test