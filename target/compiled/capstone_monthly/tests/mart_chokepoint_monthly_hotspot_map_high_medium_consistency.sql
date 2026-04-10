select *
from `capfractal`.`analytics_marts`.`mart_chokepoint_monthly_hotspot_map`
where high_medium_exposed_trade_value_usd > total_exposed_trade_value_usd + 0.000001
   or high_medium_exposed_reporter_count > exposed_reporter_count