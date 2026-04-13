
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select year_month
from `fullcap-10111`.`analytics_marts`.`mart_chokepoint_daily_signal`
where year_month is null



  
  
      
    ) dbt_internal_test