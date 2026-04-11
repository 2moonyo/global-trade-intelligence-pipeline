
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select chokepoint_name
from `chokepoint-capfractal`.`analytics_marts`.`mart_chokepoint_daily_signal`
where chokepoint_name is null



  
  
      
    ) dbt_internal_test