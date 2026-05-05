select *
from {{ ref('mart_dataset_coverage_monthly') }}
where coverage_ratio < 0
   or coverage_ratio > 1
