select *
from {{ ref('mart_executive_monthly_system_snapshot') }}
where missing_chokepoint_count <> expected_chokepoint_count - observed_chokepoint_count
   or (coverage_gap_flag and missing_chokepoint_count = 0)
   or (not coverage_gap_flag and missing_chokepoint_count > 0)
