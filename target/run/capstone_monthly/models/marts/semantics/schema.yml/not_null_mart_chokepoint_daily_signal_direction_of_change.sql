
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select direction_of_change
from `chokepoint-capfractal`.`analytics_marts`.`mart_chokepoint_daily_signal`
where direction_of_change is null



  
  
      
    ) dbt_internal_test