
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select latest_month_flag
from `capfractal`.`analytics_marts`.`mart_executive_monthly_system_snapshot`
where latest_month_flag is null



  
  
      
    ) dbt_internal_test