select
  month_start_date,
  count(*) as row_count
from {{ ref('mart_global_monthly_system_stress_summary') }}
group by 1
having count(*) > 1
