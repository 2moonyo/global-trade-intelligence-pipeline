-- Test: mart_country_event_impact must be unique at reporter_country_code + event_id + year_month_key + year_month.

select
  reporter_country_code,
  event_id,
  year_month_key,
  year_month,
  count(*) as row_count
from {{ ref('mart_country_event_impact') }}
group by 1, 2, 3, 4
having count(*) > 1
