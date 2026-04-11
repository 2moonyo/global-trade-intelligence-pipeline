
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  -- Test: reporting completeness ratio must remain between 0 and 1.

select
  *
from `chokepoint-capfractal`.`analytics_marts`.`mart_dashboard_global_trade_overview`
where reporting_completeness_pct < 0
   or reporting_completeness_pct > 1
  
  
      
    ) dbt_internal_test