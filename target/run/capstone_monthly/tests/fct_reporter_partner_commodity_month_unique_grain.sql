
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  -- Fails when canonical trade fact has duplicate rows at its declared grain.
with duplicate_grain as (
  select
    reporter_iso3,
    partner_iso3,
    cmd_code,
    period,
    year_month,
    trade_flow,
    count(*) as row_count
  from "analytics"."analytics_marts"."fct_reporter_partner_commodity_month"
  group by 1, 2, 3, 4, 5, 6
  having count(*) > 1
)

select *
from duplicate_grain
  
  
      
    ) dbt_internal_test