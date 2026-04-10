
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  -- Test: missing_from_latest_month_flag must exactly identify rows in the latest month with no upstream trade row.

select
  *
from `capfractal`.`analytics_marts`.`mart_dashboard_global_trade_overview`
where (
  missing_from_latest_month_flag
  and (
    not latest_month_flag
    or has_reported_trade_data_flag
  )
)
or (
  latest_month_flag
  and not has_reported_trade_data_flag
  and not missing_from_latest_month_flag
)
  
  
      
    ) dbt_internal_test