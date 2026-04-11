
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select year_month_key
from `chokepoint-capfractal`.`analytics_marts`.`mart_trade_month_coverage_status`
where year_month_key is null



  
  
      
    ) dbt_internal_test