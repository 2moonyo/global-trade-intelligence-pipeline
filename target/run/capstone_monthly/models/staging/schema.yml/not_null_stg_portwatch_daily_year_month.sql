
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select year_month
from `fullcap-10111`.`analytics_staging`.`stg_portwatch_daily`
where year_month is null



  
  
      
    ) dbt_internal_test