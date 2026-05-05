select
  bloc_code,
  month_start_date,
  food_share_of_bloc_trade_pct,
  energy_share_of_bloc_trade_pct,
  oil_share_of_bloc_trade_pct,
  import_dependency_proxy_pct,
  export_orientation_proxy_pct
from {{ ref('mart_bloc_month_trade_macro_summary') }}
where (food_share_of_bloc_trade_pct is not null and (food_share_of_bloc_trade_pct < 0 or food_share_of_bloc_trade_pct > 1))
   or (energy_share_of_bloc_trade_pct is not null and (energy_share_of_bloc_trade_pct < 0 or energy_share_of_bloc_trade_pct > 1))
   or (oil_share_of_bloc_trade_pct is not null and (oil_share_of_bloc_trade_pct < 0 or oil_share_of_bloc_trade_pct > 1))
   or (import_dependency_proxy_pct is not null and (import_dependency_proxy_pct < 0 or import_dependency_proxy_pct > 1))
   or (export_orientation_proxy_pct is not null and (export_orientation_proxy_pct < 0 or export_orientation_proxy_pct > 1))
