
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select chokepoint_id
from `capfractal`.`analytics_marts`.`mart_chokepoint_monthly_hotspot_map`
where chokepoint_id is null



  
  
      
    ) dbt_internal_test