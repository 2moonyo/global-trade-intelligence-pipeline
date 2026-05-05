select
  reporter_iso3,
  month_start_date,
  energy_trade_share_pct,
  oil_trade_share_pct,
  food_trade_share_pct,
  food_import_share_pct,
  oil_import_share_pct
from {{ ref('mart_reporter_month_energy_trade_dependency') }}
where (energy_trade_share_pct is not null and (energy_trade_share_pct < 0 or energy_trade_share_pct > 1))
   or (oil_trade_share_pct is not null and (oil_trade_share_pct < 0 or oil_trade_share_pct > 1))
   or (food_trade_share_pct is not null and (food_trade_share_pct < 0 or food_trade_share_pct > 1))
   or (food_import_share_pct is not null and (food_import_share_pct < 0 or food_import_share_pct > 1))
   or (oil_import_share_pct is not null and (oil_import_share_pct < 0 or oil_import_share_pct > 1))
