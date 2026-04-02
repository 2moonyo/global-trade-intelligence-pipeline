
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select ref_date
from `capfractal`.`analytics_staging`.`stg_comtrade_trade_base`
where ref_date is null



  
  
      
    ) dbt_internal_test