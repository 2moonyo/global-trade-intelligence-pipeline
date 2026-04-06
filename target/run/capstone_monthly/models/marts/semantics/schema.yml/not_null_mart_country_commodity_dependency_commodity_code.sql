
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select commodity_code
from `capfractal`.`analytics_marts`.`mart_country_commodity_dependency`
where commodity_code is null



  
  
      
    ) dbt_internal_test