
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  -- Fails when mart_global_daily_market_signal has duplicate rows at its declared grain.
with duplicate_grain as (
  select
    date_day,
    count(*) as row_count
  from `chokepoint-capfractal`.`analytics_marts`.`mart_global_daily_market_signal`
  group by 1
  having count(*) > 1
)

select *
from duplicate_grain
  
  
      
    ) dbt_internal_test