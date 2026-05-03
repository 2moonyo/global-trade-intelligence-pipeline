select
  dataset_name,
  count(*) as row_count
from {{ ref('mart_dataset_coverage_summary') }}
group by 1
having count(*) > 1
