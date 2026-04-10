
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select year_month_label
from `capfractal`.`analytics_marts`.`mart_global_daily_market_signal`
where year_month_label is null



  
  
      
    ) dbt_internal_test