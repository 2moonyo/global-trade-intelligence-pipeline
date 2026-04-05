
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select month_key
from `capfractal`.`analytics_marts`.`dim_time`
where month_key is null



  
  
      
    ) dbt_internal_test