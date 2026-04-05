
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select year_month
from `capfractal`.`analytics_marts`.`dim_time`
where year_month is null



  
  
      
    ) dbt_internal_test