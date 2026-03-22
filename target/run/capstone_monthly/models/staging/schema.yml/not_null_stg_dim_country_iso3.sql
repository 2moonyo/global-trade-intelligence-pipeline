
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select iso3
from "analytics"."analytics_staging"."stg_dim_country"
where iso3 is null



  
  
      
    ) dbt_internal_test