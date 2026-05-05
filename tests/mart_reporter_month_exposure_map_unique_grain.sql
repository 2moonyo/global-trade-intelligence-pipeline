select
  reporter_iso3,
  month_start_date,
  count(*) as row_count
from {{ ref('mart_reporter_month_exposure_map') }}
group by 1, 2
having count(*) > 1
