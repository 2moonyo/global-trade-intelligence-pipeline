
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  -- Fails when mart_macro_monthly_features has duplicate rows at its declared grain.
with duplicate_grain as (
  select
    year_month,
    currency_view,
    base_currency_code,
    fx_currency_code,
    count(*) as row_count
  from `chokepoint-capfractal`.`analytics_marts`.`mart_macro_monthly_features`
  group by 1, 2, 3, 4
  having count(*) > 1
)

select *
from duplicate_grain
  
  
      
    ) dbt_internal_test