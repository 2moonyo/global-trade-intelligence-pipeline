
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select trade_value_usd
from `capfractal`.`analytics_staging`.`stg_comtrade_trade_base`
where trade_value_usd is null



  
  
      
    ) dbt_internal_test