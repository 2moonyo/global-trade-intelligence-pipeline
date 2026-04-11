-- Fails when stg_fx_monthly has duplicate rows at its declared grain.
with duplicate_grain as (
  select
    year_month,
    currency_view,
    base_currency_code,
    fx_currency_code,
    count(*) as row_count
  from `chokepoint-capfractal`.`analytics_staging`.`stg_fx_monthly`
  group by 1, 2, 3, 4
  having count(*) > 1
)

select *
from duplicate_grain