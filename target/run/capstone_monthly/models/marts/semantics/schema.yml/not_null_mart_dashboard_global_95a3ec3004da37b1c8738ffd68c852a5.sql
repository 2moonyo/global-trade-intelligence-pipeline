
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select reporters_with_data_in_month
from `chokepoint-capfractal`.`analytics_marts`.`mart_dashboard_global_trade_overview`
where reporters_with_data_in_month is null



  
  
      
    ) dbt_internal_test