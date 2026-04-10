
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select partner_country_code
from `capfractal`.`analytics_marts`.`mart_trade_route_geography`
where partner_country_code is null



  
  
      
    ) dbt_internal_test