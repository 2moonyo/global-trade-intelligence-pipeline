
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select base_currency_code
from `capfractal`.`analytics_marts`.`mart_reporter_month_macro_features`
where base_currency_code is null



  
  
      
    ) dbt_internal_test