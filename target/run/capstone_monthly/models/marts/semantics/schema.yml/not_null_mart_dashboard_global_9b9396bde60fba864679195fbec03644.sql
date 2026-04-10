
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select expected_reporter_count
from `capfractal`.`analytics_marts`.`mart_dashboard_global_trade_overview`
where expected_reporter_count is null



  
  
      
    ) dbt_internal_test