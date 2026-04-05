
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select currency_view
from `capfractal`.`analytics_staging`.`stg_fx_monthly`
where currency_view is null



  
  
      
    ) dbt_internal_test