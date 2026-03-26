
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  -- Fails when hub dependency mart has duplicate rows at its declared grain.
with duplicate_grain as (
  select
    hub_iso3,
    period,
    year_month,
    route_confidence_score,
    count(*) as row_count
  from "analytics"."analytics_marts"."mart_hub_dependency_month"
  group by 1, 2, 3, 4
  having count(*) > 1
)

select *
from duplicate_grain
  
  
      
    ) dbt_internal_test