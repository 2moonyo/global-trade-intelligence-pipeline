
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  select *
from `chokepoint-capfractal`.`analytics_staging`.`stg_portwatch_stress_metrics`
where chokepoint_id != 
    to_hex(md5(cast(lower(trim(chokepoint_name)) as string)))
  
  
  
      
    ) dbt_internal_test