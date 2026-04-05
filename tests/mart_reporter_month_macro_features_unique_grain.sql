-- Fails when mart_reporter_month_macro_features has duplicate rows at its declared grain.
with duplicate_grain as (
  select
    reporter_iso3,
    period,
    year_month,
    currency_view,
    base_currency_code,
    fx_currency_code,
    count(*) as row_count
  from {{ ref('mart_reporter_month_macro_features') }}
  group by 1, 2, 3, 4, 5, 6
  having count(*) > 1
)

select *
from duplicate_grain
