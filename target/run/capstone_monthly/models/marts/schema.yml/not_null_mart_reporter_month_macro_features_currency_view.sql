
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select currency_view
from `capfractal`.`analytics_marts`.`mart_reporter_month_macro_features`
where currency_view is null



  
  
      
    ) dbt_internal_test