
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select year_month
from "analytics"."analytics_staging"."stg_comtrade_fact"
where year_month is null



  
  
      
    ) dbt_internal_test