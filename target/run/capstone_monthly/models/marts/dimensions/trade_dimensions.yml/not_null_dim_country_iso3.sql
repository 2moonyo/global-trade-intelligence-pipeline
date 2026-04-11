
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select iso3
from `chokepoint-capfractal`.`analytics_marts`.`dim_country`
where iso3 is null



  
  
      
    ) dbt_internal_test