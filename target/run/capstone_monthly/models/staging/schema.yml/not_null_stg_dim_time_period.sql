
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select period
from `capfractal`.`analytics_staging`.`stg_dim_time`
where period is null



  
  
      
    ) dbt_internal_test