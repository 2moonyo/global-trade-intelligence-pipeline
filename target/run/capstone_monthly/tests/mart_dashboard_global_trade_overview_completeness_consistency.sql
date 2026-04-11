
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  -- Test: complete_month_flag must exactly match month-level reporter coverage equality.

select
  *
from `chokepoint-capfractal`.`analytics_marts`.`mart_dashboard_global_trade_overview`
where (
  complete_month_flag
  and reporters_with_data_in_month <> expected_reporter_count
)
or (
  not complete_month_flag
  and reporters_with_data_in_month = expected_reporter_count
)
  
  
      
    ) dbt_internal_test