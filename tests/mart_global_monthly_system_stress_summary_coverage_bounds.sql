-- Fails when monthly coverage falls outside the valid 0 to 1 range.
select *
from {{ ref('mart_global_monthly_system_stress_summary') }}
where monthly_coverage_ratio < 0
   or monthly_coverage_ratio > 1
