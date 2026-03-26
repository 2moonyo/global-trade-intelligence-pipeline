
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select period
from "analytics"."analytics_marts"."mart_hub_dependency_month"
where period is null



  
  
      
    ) dbt_internal_test