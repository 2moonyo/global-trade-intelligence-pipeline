
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select currency_view
from `capfractal`.`analytics_marts`.`mart_macro_monthly_features`
where currency_view is null



  
  
      
    ) dbt_internal_test