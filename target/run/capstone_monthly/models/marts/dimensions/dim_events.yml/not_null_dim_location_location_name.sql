
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select location_name
from `chokepoint-capfractal`.`analytics_analytics_marts`.`dim_location`
where location_name is null



  
  
      
    ) dbt_internal_test