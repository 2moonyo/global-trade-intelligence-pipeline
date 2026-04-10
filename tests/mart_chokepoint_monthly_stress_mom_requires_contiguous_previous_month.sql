-- Fails when month-over-month fields are populated without an immediately prior calendar month.
select *
from {{ ref('mart_chokepoint_monthly_stress') }}
where previous_month_available_flag = false
  and (
    previous_month_throughput_metric is not null
    or previous_month_vessel_count_metric is not null
    or previous_month_stress_index is not null
    or previous_month_stress_index_weighted is not null
    or throughput_mom_change is not null
    or throughput_mom_change_pct is not null
    or vessel_count_mom_change is not null
    or vessel_count_mom_change_pct is not null
    or stress_index_mom_change is not null
    or stress_index_weighted_mom_change is not null
  )
