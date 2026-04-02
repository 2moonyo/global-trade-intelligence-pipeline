
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select hub_iso3
from `capfractal`.`analytics_marts`.`mart_hub_dependency_month`
where hub_iso3 is null



  
  
      
    ) dbt_internal_test