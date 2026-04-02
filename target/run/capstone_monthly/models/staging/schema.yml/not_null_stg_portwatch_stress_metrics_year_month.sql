
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select year_month
from `capfractal`.`analytics_staging`.`stg_portwatch_stress_metrics`
where year_month is null



  
  
      
    ) dbt_internal_test