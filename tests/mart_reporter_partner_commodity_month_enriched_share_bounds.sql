select *
from {{ ref('mart_reporter_partner_commodity_month_enriched') }}
where (
    chokepoint_exposed_trade_share_of_reporter_total is not null
    and (
      chokepoint_exposed_trade_share_of_reporter_total < 0
      or chokepoint_exposed_trade_share_of_reporter_total > 1.000001
    )
  )
  or (
    partner_commodity_trade_share_of_reporter_chokepoint is not null
    and (
      partner_commodity_trade_share_of_reporter_chokepoint < 0
      or partner_commodity_trade_share_of_reporter_chokepoint > 1.000001
    )
  )
