
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select year_month
from "analytics"."analytics_marts"."mart_trade_exposure"
where year_month is null



  
  
      
    ) dbt_internal_test