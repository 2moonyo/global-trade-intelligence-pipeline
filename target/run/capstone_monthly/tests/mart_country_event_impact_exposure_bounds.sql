
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  -- Test: event exposure percentage should remain within 0-100 bounds.

select
  reporter_country_code,
  event_id,
  year_month_key,
  event_exposure_pct
from `capfractal`.`analytics_marts`.`mart_country_event_impact`
where event_exposure_pct < 0
   or event_exposure_pct > 100
  
  
      
    ) dbt_internal_test