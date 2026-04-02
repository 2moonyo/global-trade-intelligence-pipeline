
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select chokepoint_id
from `capfractal`.`analytics_marts`.`mart_trade_exposure`
where chokepoint_id is null



  
  
      
    ) dbt_internal_test