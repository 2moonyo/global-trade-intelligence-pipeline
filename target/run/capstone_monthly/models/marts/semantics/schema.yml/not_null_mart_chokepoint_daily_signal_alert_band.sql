
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select alert_band
from `fullcap-10111`.`analytics_marts`.`mart_chokepoint_daily_signal`
where alert_band is null



  
  
      
    ) dbt_internal_test