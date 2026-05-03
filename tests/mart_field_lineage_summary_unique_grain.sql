select
  dashboard_page,
  field_name,
  count(*) as row_count
from {{ ref('mart_field_lineage_summary') }}
group by 1, 2
having count(*) > 1
