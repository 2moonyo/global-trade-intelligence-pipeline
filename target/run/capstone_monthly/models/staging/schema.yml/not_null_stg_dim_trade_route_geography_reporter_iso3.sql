
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select reporter_iso3
from `capfractal`.`analytics_staging`.`stg_dim_trade_route_geography`
where reporter_iso3 is null



  
  
      
    ) dbt_internal_test