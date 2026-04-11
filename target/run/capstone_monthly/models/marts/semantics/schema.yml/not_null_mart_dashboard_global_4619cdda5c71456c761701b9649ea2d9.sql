
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select reporting_completeness_pct
from `chokepoint-capfractal`.`analytics_marts`.`mart_dashboard_global_trade_overview`
where reporting_completeness_pct is null



  
  
      
    ) dbt_internal_test