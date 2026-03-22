
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select period
from "analytics"."analytics_marts"."mart_reporter_commodity_month_trade_summary"
where period is null



  
  
      
    ) dbt_internal_test