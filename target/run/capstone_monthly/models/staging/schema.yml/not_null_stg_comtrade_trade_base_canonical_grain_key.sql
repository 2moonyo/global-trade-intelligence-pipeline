
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select canonical_grain_key
from `chokepoint-capfractal`.`analytics_staging`.`stg_comtrade_trade_base`
where canonical_grain_key is null



  
  
      
    ) dbt_internal_test