
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select chokepoint_name
from `fullcap-10111`.`analytics_staging`.`stg_portwatch_daily`
where chokepoint_name is null



  
  
      
    ) dbt_internal_test