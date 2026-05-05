select
  reporter_iso3,
  count(*) as row_count
from {{ ref('mart_reporter_month_exposure_map') }}
where latest_month_flag
group by 1
having count(*) > 1
