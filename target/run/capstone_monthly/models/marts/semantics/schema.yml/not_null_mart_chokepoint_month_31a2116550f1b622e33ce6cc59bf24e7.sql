
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select top_5_stressed_chokepoint_flag
from `capfractal`.`analytics_marts`.`mart_chokepoint_monthly_stress_detail`
where top_5_stressed_chokepoint_flag is null



  
  
      
    ) dbt_internal_test