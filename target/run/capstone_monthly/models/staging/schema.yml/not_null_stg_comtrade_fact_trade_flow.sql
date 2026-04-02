
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select trade_flow
from `capfractal`.`analytics_staging`.`stg_comtrade_fact`
where trade_flow is null



  
  
      
    ) dbt_internal_test