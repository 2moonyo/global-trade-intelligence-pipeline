select
  month_start_date,
  chokepoint_id,
  count(*) as row_count
from {{ ref('mart_chokepoint_monthly_hotspot_map') }}
group by 1, 2
having count(*) > 1
