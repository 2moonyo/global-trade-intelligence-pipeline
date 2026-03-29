
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  select *
from "analytics"."analytics_staging"."stg_portwatch_stress_metrics"
where chokepoint_id != md5(lower(trim(chokepoint_name)))
  
  
      
    ) dbt_internal_test