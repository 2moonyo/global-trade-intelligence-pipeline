
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select cmd_code
from `capfractal`.`analytics_staging`.`stg_comtrade_trade_base`
where cmd_code is null



  
  
      
    ) dbt_internal_test