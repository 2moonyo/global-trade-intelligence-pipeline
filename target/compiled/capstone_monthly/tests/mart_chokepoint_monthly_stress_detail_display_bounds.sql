select *
from `capfractal`.`analytics_marts`.`mart_chokepoint_monthly_stress_detail`
where (
    z_score_historical_capped is not null
    and (
      z_score_historical_capped < -3
      or z_score_historical_capped > 3
    )
  )
  or (
    stress_deviation_score_capped is not null
    and (
      stress_deviation_score_capped < 0
      or stress_deviation_score_capped > 3
    )
  )
  or (
    stress_deviation_index_100 is not null
    and (
      stress_deviation_index_100 < 0
      or stress_deviation_index_100 > 100
    )
  )