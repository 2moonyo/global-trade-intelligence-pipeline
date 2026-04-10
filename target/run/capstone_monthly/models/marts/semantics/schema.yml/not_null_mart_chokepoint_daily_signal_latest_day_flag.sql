
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select latest_day_flag
from `capfractal`.`analytics_marts`.`mart_chokepoint_daily_signal`
where latest_day_flag is null



  
  
      
    ) dbt_internal_test