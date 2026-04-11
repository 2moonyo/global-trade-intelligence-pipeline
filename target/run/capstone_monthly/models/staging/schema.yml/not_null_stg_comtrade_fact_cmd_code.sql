
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select cmd_code
from `chokepoint-capfractal`.`analytics_staging`.`stg_comtrade_fact`
where cmd_code is null



  
  
      
    ) dbt_internal_test