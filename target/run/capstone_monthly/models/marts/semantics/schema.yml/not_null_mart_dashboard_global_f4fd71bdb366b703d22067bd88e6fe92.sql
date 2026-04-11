
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select has_reported_trade_data_flag
from `chokepoint-capfractal`.`analytics_marts`.`mart_dashboard_global_trade_overview`
where has_reported_trade_data_flag is null



  
  
      
    ) dbt_internal_test