
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select observed_chokepoint_count
from `capfractal`.`analytics_marts`.`mart_global_daily_market_signal`
where observed_chokepoint_count is null



  
  
      
    ) dbt_internal_test