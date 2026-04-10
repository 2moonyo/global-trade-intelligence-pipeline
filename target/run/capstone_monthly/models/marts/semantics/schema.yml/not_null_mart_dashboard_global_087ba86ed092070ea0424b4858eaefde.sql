
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select latest_complete_month_flag
from `capfractal`.`analytics_marts`.`mart_dashboard_global_trade_overview`
where latest_complete_month_flag is null



  
  
      
    ) dbt_internal_test