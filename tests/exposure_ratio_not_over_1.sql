select *
from {{ ref('mart_reporter_month_chokepoint_exposure') }}
where chokepoint_trade_exposure_ratio < 0