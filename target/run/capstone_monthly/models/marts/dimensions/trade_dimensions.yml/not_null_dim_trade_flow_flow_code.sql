
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select flow_code
from `capfractal`.`analytics_marts`.`dim_trade_flow`
where flow_code is null



  
  
      
    ) dbt_internal_test