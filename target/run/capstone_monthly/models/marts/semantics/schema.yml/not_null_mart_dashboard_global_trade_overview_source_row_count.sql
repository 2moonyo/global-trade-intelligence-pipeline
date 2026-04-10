
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select source_row_count
from `capfractal`.`analytics_marts`.`mart_dashboard_global_trade_overview`
where source_row_count is null



  
  
      
    ) dbt_internal_test