
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select period
from `chokepoint-capfractal`.`analytics_marts`.`mart_reporter_month_chokepoint_exposure`
where period is null



  
  
      
    ) dbt_internal_test