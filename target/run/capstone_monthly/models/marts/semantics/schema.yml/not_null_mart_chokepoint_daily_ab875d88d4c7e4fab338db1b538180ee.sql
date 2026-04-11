
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select has_portwatch_daily_data_flag
from `chokepoint-capfractal`.`analytics_marts`.`mart_chokepoint_daily_signal`
where has_portwatch_daily_data_flag is null



  
  
      
    ) dbt_internal_test