select *
from {{ ref('mart_reporter_month_exposure_map') }}
where (
    total_chokepoint_exposed_trade_share is not null
    and (
      total_chokepoint_exposed_trade_share < 0
      or total_chokepoint_exposed_trade_share > 1.000001
    )
  )
  or (
    high_medium_chokepoint_exposed_trade_share is not null
    and (
      high_medium_chokepoint_exposed_trade_share < 0
      or high_medium_chokepoint_exposed_trade_share > 1.000001
      or high_medium_chokepoint_exposed_trade_share > total_chokepoint_exposed_trade_share + 0.000001
    )
  )
