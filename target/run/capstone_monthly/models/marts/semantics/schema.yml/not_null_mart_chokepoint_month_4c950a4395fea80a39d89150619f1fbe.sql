
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select stress_severity_band
from `capfractal`.`analytics_marts`.`mart_chokepoint_monthly_hotspot_map`
where stress_severity_band is null



  
  
      
    ) dbt_internal_test