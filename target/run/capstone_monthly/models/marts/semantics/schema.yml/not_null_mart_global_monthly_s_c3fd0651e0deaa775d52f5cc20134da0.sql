
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select latest_month_flag
from `chokepoint-capfractal`.`analytics_marts`.`mart_global_monthly_system_stress_summary`
where latest_month_flag is null



  
  
      
    ) dbt_internal_test