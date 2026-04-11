
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select portwatch_source_chokepoint_id
from `chokepoint-capfractal`.`analytics_staging`.`stg_portwatch_daily`
where portwatch_source_chokepoint_id is null



  
  
      
    ) dbt_internal_test