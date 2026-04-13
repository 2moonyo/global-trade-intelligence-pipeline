
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select has_portwatch_daily_data_flag
from `fullcap-10111`.`analytics_staging`.`stg_portwatch_daily`
where has_portwatch_daily_data_flag is null



  
  
      
    ) dbt_internal_test