
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select period
from `chokepoint-capfractal`.`analytics_staging`.`stg_comtrade_fact`
where period is null



  
  
      
    ) dbt_internal_test