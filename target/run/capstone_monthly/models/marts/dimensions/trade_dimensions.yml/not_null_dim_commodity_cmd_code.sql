
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select cmd_code
from `chokepoint-capfractal`.`analytics_marts`.`dim_commodity`
where cmd_code is null



  
  
      
    ) dbt_internal_test