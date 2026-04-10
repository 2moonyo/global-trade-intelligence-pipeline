
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select reporting_completeness_pct
from `capfractal`.`analytics_marts`.`mart_trade_month_coverage_status`
where reporting_completeness_pct is null



  
  
      
    ) dbt_internal_test