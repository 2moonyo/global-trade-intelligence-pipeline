
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  select
  month_start_date,
  count(*) as row_count
from `capfractal`.`analytics_marts`.`mart_global_monthly_system_stress_summary`
group by 1
having count(*) > 1
  
  
      
    ) dbt_internal_test