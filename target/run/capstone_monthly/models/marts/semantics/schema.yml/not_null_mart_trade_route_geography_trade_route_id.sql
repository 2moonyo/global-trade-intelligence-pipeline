
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select trade_route_id
from `capfractal`.`analytics_marts`.`mart_trade_route_geography`
where trade_route_id is null



  
  
      
    ) dbt_internal_test