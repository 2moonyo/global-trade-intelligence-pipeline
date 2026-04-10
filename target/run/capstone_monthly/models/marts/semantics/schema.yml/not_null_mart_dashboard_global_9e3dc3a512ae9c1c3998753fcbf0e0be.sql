
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select export_trade_value_usd
from `capfractal`.`analytics_marts`.`mart_dashboard_global_trade_overview`
where export_trade_value_usd is null



  
  
      
    ) dbt_internal_test