
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select fx_currency_code
from `capfractal`.`analytics_marts`.`mart_macro_monthly_features`
where fx_currency_code is null



  
  
      
    ) dbt_internal_test