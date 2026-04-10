
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select latest_observed_month_start_date
from `capfractal`.`analytics_marts`.`mart_chokepoint_monthly_stress`
where latest_observed_month_start_date is null



  
  
      
    ) dbt_internal_test