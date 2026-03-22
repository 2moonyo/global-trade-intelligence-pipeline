
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select partner_iso3
from "analytics"."analytics_staging"."stg_comtrade_trade_base"
where partner_iso3 is null



  
  
      
    ) dbt_internal_test