
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select import_trade_value_usd
from `capfractal`.`analytics_marts`.`mart_dashboard_global_trade_overview`
where import_trade_value_usd is null



  
  
      
    ) dbt_internal_test