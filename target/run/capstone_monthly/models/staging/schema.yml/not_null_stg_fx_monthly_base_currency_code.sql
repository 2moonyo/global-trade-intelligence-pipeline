
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select base_currency_code
from `capfractal`.`analytics_staging`.`stg_fx_monthly`
where base_currency_code is null



  
  
      
    ) dbt_internal_test