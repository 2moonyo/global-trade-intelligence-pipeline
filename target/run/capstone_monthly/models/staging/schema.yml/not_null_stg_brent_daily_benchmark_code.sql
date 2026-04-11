
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select benchmark_code
from `chokepoint-capfractal`.`analytics_staging`.`stg_brent_daily`
where benchmark_code is null



  
  
      
    ) dbt_internal_test