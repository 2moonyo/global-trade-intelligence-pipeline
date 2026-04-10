
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  -- Fails when daily coverage falls outside the valid 0 to 1 range.
select *
from `capfractal`.`analytics_marts`.`mart_global_daily_market_signal`
where daily_coverage_ratio < 0
   or daily_coverage_ratio > 1
  
  
      
    ) dbt_internal_test