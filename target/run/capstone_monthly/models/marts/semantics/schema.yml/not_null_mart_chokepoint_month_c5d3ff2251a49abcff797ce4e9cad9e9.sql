
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select latest_observed_month_flag
from `capfractal`.`analytics_marts`.`mart_chokepoint_monthly_stress`
where latest_observed_month_flag is null



  
  
      
    ) dbt_internal_test