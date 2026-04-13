
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select chokepoint_id
from `fullcap-10111`.`analytics_marts`.`mart_chokepoint_daily_signal`
where chokepoint_id is null



  
  
      
    ) dbt_internal_test