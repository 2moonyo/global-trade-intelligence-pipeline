-- Test: mart_country_trade_profile must be unique at reporter_country_code + year_month_key + year_month.

select
  reporter_country_code,
  year_month_key,
  year_month,
  count(*) as row_count
from {{ ref('mart_country_trade_profile') }}
group by 1, 2, 3
having count(*) > 1
