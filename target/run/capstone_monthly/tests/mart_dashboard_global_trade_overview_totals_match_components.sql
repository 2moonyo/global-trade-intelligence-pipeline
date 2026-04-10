
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  -- Test: where upstream rows exist, total trade must equal import plus export within a small tolerance.

select
  *
from `capfractal`.`analytics_marts`.`mart_dashboard_global_trade_overview`
where has_reported_trade_data_flag
  and abs(total_trade_value_usd - (import_trade_value_usd + export_trade_value_usd)) > 0.01
  
  
      
    ) dbt_internal_test