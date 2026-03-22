
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select reporter_iso3
from "analytics"."analytics_staging"."stg_comtrade_trade_base"
where reporter_iso3 is null



  
  
      
    ) dbt_internal_test