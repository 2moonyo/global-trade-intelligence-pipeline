
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select reporter_country_name
from `capfractal`.`analytics_marts`.`mart_reporter_month_exposure_map`
where reporter_country_name is null



  
  
      
    ) dbt_internal_test