
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  select *
from `capfractal`.`analytics_marts`.`mart_trade_month_coverage_status`
where reporting_completeness_pct < 0
   or reporting_completeness_pct > 1
   or missing_reporter_pct < 0
   or missing_reporter_pct > 1
  
  
      
    ) dbt_internal_test