
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select coverage_gap_flag
from `chokepoint-capfractal`.`analytics_marts`.`mart_trade_month_coverage_status`
where coverage_gap_flag is null



  
  
      
    ) dbt_internal_test