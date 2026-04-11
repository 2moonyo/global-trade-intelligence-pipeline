
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select reporter_iso3
from `chokepoint-capfractal`.`analytics_marts`.`mart_reporter_month_exposure_map`
where reporter_iso3 is null



  
  
      
    ) dbt_internal_test