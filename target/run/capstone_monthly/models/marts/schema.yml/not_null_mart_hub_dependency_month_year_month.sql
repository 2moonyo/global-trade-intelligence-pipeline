
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select year_month
from `chokepoint-capfractal`.`analytics_marts`.`mart_hub_dependency_month`
where year_month is null



  
  
      
    ) dbt_internal_test