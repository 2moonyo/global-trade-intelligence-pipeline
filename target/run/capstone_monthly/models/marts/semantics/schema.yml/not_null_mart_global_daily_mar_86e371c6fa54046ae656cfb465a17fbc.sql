
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select daily_source_coverage_status
from `capfractal`.`analytics_marts`.`mart_global_daily_market_signal`
where daily_source_coverage_status is null



  
  
      
    ) dbt_internal_test