
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select system_stress_level
from `chokepoint-capfractal`.`analytics_marts`.`mart_global_daily_market_signal`
where system_stress_level is null



  
  
      
    ) dbt_internal_test