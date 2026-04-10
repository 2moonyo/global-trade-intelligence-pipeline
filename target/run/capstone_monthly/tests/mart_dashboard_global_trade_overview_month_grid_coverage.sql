
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  -- Test: each month must retain one row for every expected reporter and repeat month-level counts consistently.

select
  month_start_date,
  count(*) as row_count,
  min(expected_reporter_count) as min_expected_reporter_count,
  max(expected_reporter_count) as max_expected_reporter_count
from `capfractal`.`analytics_marts`.`mart_dashboard_global_trade_overview`
group by 1
having count(*) <> min(expected_reporter_count)
   or min(expected_reporter_count) <> max(expected_reporter_count)
  
  
      
    ) dbt_internal_test