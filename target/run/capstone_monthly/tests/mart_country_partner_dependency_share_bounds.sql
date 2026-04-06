
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  -- Test: partner share metrics should remain within 0-100 bounds.

select
  reporter_country_code,
  partner_country_code,
  year_month_key,
  partner_trade_share_pct,
  cumulative_partner_share_pct
from `capfractal`.`analytics_marts`.`mart_country_partner_dependency`
where partner_trade_share_pct < 0
   or partner_trade_share_pct > 100
   or cumulative_partner_share_pct < 0
   or cumulative_partner_share_pct > 100
  
  
      
    ) dbt_internal_test