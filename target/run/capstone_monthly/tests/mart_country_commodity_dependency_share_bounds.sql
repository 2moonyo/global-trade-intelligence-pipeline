
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  -- Test: commodity share metrics should remain within 0-100 bounds.

select
  reporter_country_code,
  commodity_code,
  year_month_key,
  commodity_trade_share_pct,
  cumulative_commodity_share_pct
from `capfractal`.`analytics_marts`.`mart_country_commodity_dependency`
where commodity_trade_share_pct < 0
   or commodity_trade_share_pct > 100
   or cumulative_commodity_share_pct < 0
   or cumulative_commodity_share_pct > 100
  
  
      
    ) dbt_internal_test