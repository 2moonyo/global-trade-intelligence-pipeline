
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select previous_month_available_flag
from `capfractal`.`analytics_marts`.`mart_chokepoint_monthly_stress_detail`
where previous_month_available_flag is null



  
  
      
    ) dbt_internal_test