-- Fails when rolling 6-month stress fields appear before there are enough prior observed months.
select *
from {{ ref('mart_chokepoint_monthly_stress') }}
where rolling_6m_baseline_observation_count < 2
  and (
    z_score_rolling_6m is not null
    or z_score_count_rolling_6m is not null
    or z_score_vessel_size_rolling_6m is not null
    or stress_index_rolling_6m is not null
    or stress_index_weighted_rolling_6m is not null
  )
