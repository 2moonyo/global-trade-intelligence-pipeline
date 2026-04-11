
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select expected_reporter_count
from `chokepoint-capfractal`.`analytics_marts`.`mart_trade_month_coverage_status`
where expected_reporter_count is null



  
  
      
    ) dbt_internal_test