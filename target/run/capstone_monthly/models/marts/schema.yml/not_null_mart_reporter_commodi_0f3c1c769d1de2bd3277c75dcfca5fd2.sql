
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select reporter_iso3
from "analytics"."analytics_marts"."mart_reporter_commodity_month_trade_summary"
where reporter_iso3 is null



  
  
      
    ) dbt_internal_test