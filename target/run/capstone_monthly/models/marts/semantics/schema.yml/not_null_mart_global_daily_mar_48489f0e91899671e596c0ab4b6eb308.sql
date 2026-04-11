
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select has_brent_price_data_flag
from `chokepoint-capfractal`.`analytics_marts`.`mart_global_daily_market_signal`
where has_brent_price_data_flag is null



  
  
      
    ) dbt_internal_test