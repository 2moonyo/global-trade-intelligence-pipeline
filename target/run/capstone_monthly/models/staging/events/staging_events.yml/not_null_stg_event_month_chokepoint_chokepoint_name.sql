
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select chokepoint_name
from `chokepoint-capfractal`.`analytics_analytics_staging`.`stg_event_month_chokepoint`
where chokepoint_name is null



  
  
      
    ) dbt_internal_test