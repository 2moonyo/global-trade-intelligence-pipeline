
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select latest_month_flag
from `capfractal`.`analytics_marts`.`mart_reporter_month_exposure_map`
where latest_month_flag is null



  
  
      
    ) dbt_internal_test