select *
from {{ ref('mart_chokepoint_monthly_hotspot_map') }}
where high_medium_exposed_trade_value_usd
    > total_exposed_trade_value_usd
      + greatest(0.01, abs(total_exposed_trade_value_usd) * 1e-12)
   or high_medium_exposed_reporter_count > exposed_reporter_count
