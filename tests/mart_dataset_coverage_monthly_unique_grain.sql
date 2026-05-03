select
  dataset_name,
  month_start_date,
  count(*) as row_count
from {{ ref('mart_dataset_coverage_monthly') }}
group by 1, 2
having count(*) > 1
