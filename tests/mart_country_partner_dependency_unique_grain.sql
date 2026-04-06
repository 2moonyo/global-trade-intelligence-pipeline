-- Test: mart_country_partner_dependency must be unique at reporter_country_code + partner_country_code + year_month_key + year_month.

select
  reporter_country_code,
  partner_country_code,
  year_month_key,
  year_month,
  count(*) as row_count
from {{ ref('mart_country_partner_dependency') }}
group by 1, 2, 3, 4
having count(*) > 1
