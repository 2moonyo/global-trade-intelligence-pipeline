
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select months_since_latest_observation
from `capfractal`.`analytics_marts`.`mart_chokepoint_monthly_stress`
where months_since_latest_observation is null



  
  
      
    ) dbt_internal_test