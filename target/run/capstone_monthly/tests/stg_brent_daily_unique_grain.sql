
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  -- Fails when stg_brent_daily has duplicate rows at its declared grain.
with duplicate_grain as (
  select
    date_day,
    benchmark_code,
    count(*) as row_count
  from `chokepoint-capfractal`.`analytics_staging`.`stg_brent_daily`
  group by 1, 2
  having count(*) > 1
)

select *
from duplicate_grain
  
  
      
    ) dbt_internal_test