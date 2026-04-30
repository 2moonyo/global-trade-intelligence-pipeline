-- Monthly co-movement mart combining Brent, FX, and bloc trade.
-- Grain: one row per currency_view + base_currency_code + fx_currency_code + bloc_code + month_start_date.
-- Correlation fields are descriptive rolling associations only and do not imply causality.

with macro_spine as (
  select
    mm.year_month,
    dt.month_start_date,
    mm.currency_view,
    mm.base_currency_code,
    mm.fx_currency_code,
    mm.fx_rate,
    mm.fx_rate_to_usd,
    mm.fx_mom_change,
    mm.brent_price_usd,
    mm.brent_mom_change,
    mm.wti_price_usd,
    mm.brent_wti_spread_usd
  from {{ ref('mart_macro_monthly_features') }} as mm
  left join {{ ref('dim_time') }} as dt
    on mm.year_month = dt.year_month
),
joined as (
  select
    ms.month_start_date,
    ms.year_month,
    ms.currency_view,
    ms.base_currency_code,
    ms.fx_currency_code,
    ms.fx_rate,
    ms.fx_rate_to_usd,
    ms.fx_mom_change,
    ms.brent_price_usd,
    ms.brent_mom_change,
    ms.wti_price_usd,
    ms.brent_wti_spread_usd,
    bm.bloc_code,
    bm.bloc_name,
    bm.bloc_total_trade_value_usd,
    bm.bloc_food_trade_value_usd,
    bm.bloc_oil_trade_value_usd,
    bm.mom_change_total_trade_pct,
    bm.mom_change_food_trade_pct,
    bm.mom_change_oil_trade_pct
  from macro_spine as ms
  inner join {{ ref('mart_bloc_month_trade_macro_summary') }} as bm
    on ms.year_month = bm.year_month
)

select
  month_start_date,
  year_month,
  currency_view,
  base_currency_code,
  fx_currency_code,
  fx_rate,
  fx_rate_to_usd,
  fx_mom_change,
  brent_price_usd,
  brent_mom_change,
  wti_price_usd,
  brent_wti_spread_usd,
  bloc_code,
  bloc_name,
  bloc_total_trade_value_usd,
  bloc_food_trade_value_usd,
  bloc_oil_trade_value_usd,
  mom_change_total_trade_pct,
  mom_change_food_trade_pct,
  mom_change_oil_trade_pct,
  {{ rolling_corr(
    'brent_mom_change',
    'fx_mom_change',
    'currency_view, base_currency_code, fx_currency_code, bloc_code',
    'month_start_date',
    6
  ) }} as rolling_6m_corr_brent_fx_mom,
  {{ rolling_corr(
    'brent_mom_change',
    'fx_mom_change',
    'currency_view, base_currency_code, fx_currency_code, bloc_code',
    'month_start_date',
    12
  ) }} as rolling_12m_corr_brent_fx_mom,
  {{ rolling_corr(
    'brent_mom_change',
    'mom_change_food_trade_pct',
    'currency_view, base_currency_code, fx_currency_code, bloc_code',
    'month_start_date',
    6
  ) }} as rolling_6m_corr_brent_food_trade_mom,
  {{ rolling_corr(
    'brent_mom_change',
    'mom_change_food_trade_pct',
    'currency_view, base_currency_code, fx_currency_code, bloc_code',
    'month_start_date',
    12
  ) }} as rolling_12m_corr_brent_food_trade_mom,
  {{ rolling_corr(
    'brent_mom_change',
    'mom_change_oil_trade_pct',
    'currency_view, base_currency_code, fx_currency_code, bloc_code',
    'month_start_date',
    6
  ) }} as rolling_6m_corr_brent_oil_trade_mom,
  {{ rolling_corr(
    'brent_mom_change',
    'mom_change_oil_trade_pct',
    'currency_view, base_currency_code, fx_currency_code, bloc_code',
    'month_start_date',
    12
  ) }} as rolling_12m_corr_brent_oil_trade_mom
from joined
