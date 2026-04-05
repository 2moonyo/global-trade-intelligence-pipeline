
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  select *
from `capfractal`.`analytics_marts`.`mart_reporter_month_chokepoint_exposure`
where chokepoint_trade_exposure_ratio < 0
  
  
      
    ) dbt_internal_test