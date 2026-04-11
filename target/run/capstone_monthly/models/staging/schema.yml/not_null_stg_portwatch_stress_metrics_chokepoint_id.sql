
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select chokepoint_id
from `chokepoint-capfractal`.`analytics_staging`.`stg_portwatch_stress_metrics`
where chokepoint_id is null



  
  
      
    ) dbt_internal_test