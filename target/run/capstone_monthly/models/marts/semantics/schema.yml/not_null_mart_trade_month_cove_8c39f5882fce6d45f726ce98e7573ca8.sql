
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select trade_reporting_status
from `chokepoint-capfractal`.`analytics_marts`.`mart_trade_month_coverage_status`
where trade_reporting_status is null



  
  
      
    ) dbt_internal_test