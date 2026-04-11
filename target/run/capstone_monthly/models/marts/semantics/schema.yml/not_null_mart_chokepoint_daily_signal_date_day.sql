
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select date_day
from `chokepoint-capfractal`.`analytics_marts`.`mart_chokepoint_daily_signal`
where date_day is null



  
  
      
    ) dbt_internal_test