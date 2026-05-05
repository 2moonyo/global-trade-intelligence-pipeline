select *
from {{ ref('mart_executive_monthly_system_snapshot') }}
where not previous_month_available_flag
  and (
    previous_month_avg_stress_index is not null
    or previous_month_avg_stress_index_weighted is not null
    or previous_month_stressed_chokepoint_count is not null
    or previous_month_event_impacted_chokepoint_count is not null
    or avg_stress_index_mom_change is not null
    or avg_stress_index_weighted_mom_change is not null
    or stressed_chokepoint_count_mom_change is not null
    or event_impacted_chokepoint_count_mom_change is not null
  )
