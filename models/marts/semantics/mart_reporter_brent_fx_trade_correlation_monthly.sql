-- Monthly co-movement mart combining Brent, FX, and reporter trade.
-- Grain: one row per currency_view + base_currency_code + fx_currency_code + reporter_iso3 + month_start_date.
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
reporter_commodity_month as (
  select
    {{ canonical_country_iso3('rcm.reporter_iso3') }} as reporter_iso3,
    rcm.cmd_code,
    rcm.year_month,
    rcm.month_start_date,
    rcm.year,
    rcm.month,
    rcm.total_trade_value_usd,
    rcm.import_trade_value_usd,
    rcm.export_trade_value_usd,
    rcm.food_flag,
    co.hs4,
    co.hs6
  from {{ ref('mart_reporter_commodity_month_trade_summary') }} as rcm
  left join {{ ref('dim_commodity') }} as co
    on rcm.cmd_code = co.cmd_code
),
reporter_trade as (
  select
    rcm.reporter_iso3,
    rcm.month_start_date,
    max(rcm.year_month) as year_month,
    max(rcm.year) as year,
    max(rcm.month) as month,
    sum(rcm.total_trade_value_usd) as reporter_total_trade_value_usd,
    sum(rcm.import_trade_value_usd) as reporter_import_trade_value_usd,
    sum(rcm.export_trade_value_usd) as reporter_export_trade_value_usd,
    sum(case when coalesce(rcm.food_flag, false) then rcm.total_trade_value_usd else 0 end) as reporter_food_trade_value_usd,
    sum(case when coalesce(rcm.food_flag, false) then rcm.import_trade_value_usd else 0 end) as reporter_food_import_value_usd,
    sum(case when coalesce(rcm.food_flag, false) then rcm.export_trade_value_usd else 0 end) as reporter_food_export_value_usd,
    sum(
      case
        when rcm.hs4 in ('2709', '2710')
          or rcm.hs6 = '271111'
          or cast(rcm.cmd_code as {{ dbt.type_string() }}) in ('2709', '2710', '271111')
          then rcm.total_trade_value_usd
        else 0
      end
    ) as reporter_oil_trade_value_usd,
    sum(
      case
        when rcm.hs4 in ('2709', '2710')
          or rcm.hs6 = '271111'
          or cast(rcm.cmd_code as {{ dbt.type_string() }}) in ('2709', '2710', '271111')
          then rcm.import_trade_value_usd
        else 0
      end
    ) as reporter_oil_import_value_usd,
    sum(
      case
        when rcm.hs4 in ('2709', '2710')
          or rcm.hs6 = '271111'
          or cast(rcm.cmd_code as {{ dbt.type_string() }}) in ('2709', '2710', '271111')
          then rcm.export_trade_value_usd
        else 0
      end
    ) as reporter_oil_export_value_usd
  from reporter_commodity_month as rcm
  group by 1, 2
),
with_previous as (
  select
    rt.*,
    lag(month_start_date, 1) over (
      partition by reporter_iso3
      order by month_start_date
    ) as previous_month_start_date,
    lag(reporter_total_trade_value_usd, 1) over (
      partition by reporter_iso3
      order by month_start_date
    ) as previous_total_trade_value_usd,
    lag(reporter_food_trade_value_usd, 1) over (
      partition by reporter_iso3
      order by month_start_date
    ) as previous_food_trade_value_usd,
    lag(reporter_oil_trade_value_usd, 1) over (
      partition by reporter_iso3
      order by month_start_date
    ) as previous_oil_trade_value_usd
  from reporter_trade as rt
),
reporter_monthly as (
  select
    wp.reporter_iso3,
    dc.country_name as reporter_country_name,
    dc.region as reporter_region,
    dc.subregion as reporter_subregion,
    dc.continent as reporter_continent,
    dc.is_eu as reporter_is_eu,
    dc.is_oecd as reporter_is_oecd,
    wp.month_start_date,
    wp.year_month,
    wp.year,
    wp.month,
    wp.reporter_total_trade_value_usd,
    wp.reporter_import_trade_value_usd,
    wp.reporter_export_trade_value_usd,
    wp.reporter_food_trade_value_usd,
    wp.reporter_food_import_value_usd,
    wp.reporter_food_export_value_usd,
    wp.reporter_oil_trade_value_usd,
    wp.reporter_oil_import_value_usd,
    wp.reporter_oil_export_value_usd,
    case
      when wp.previous_month_start_date is not null
        and {{ date_add_months('wp.previous_month_start_date', 1) }} = wp.month_start_date
        then {{ safe_divide(
          'wp.reporter_total_trade_value_usd - wp.previous_total_trade_value_usd',
          'wp.previous_total_trade_value_usd'
        ) }}
      else null
    end as mom_change_total_trade_pct,
    case
      when wp.previous_month_start_date is not null
        and {{ date_add_months('wp.previous_month_start_date', 1) }} = wp.month_start_date
        then {{ safe_divide(
          'wp.reporter_food_trade_value_usd - wp.previous_food_trade_value_usd',
          'wp.previous_food_trade_value_usd'
        ) }}
      else null
    end as mom_change_food_trade_pct,
    case
      when wp.previous_month_start_date is not null
        and {{ date_add_months('wp.previous_month_start_date', 1) }} = wp.month_start_date
        then {{ safe_divide(
          'wp.reporter_oil_trade_value_usd - wp.previous_oil_trade_value_usd',
          'wp.previous_oil_trade_value_usd'
        ) }}
      else null
    end as mom_change_oil_trade_pct
  from with_previous as wp
  left join {{ ref('dim_country') }} as dc
    on wp.reporter_iso3 = dc.iso3
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
    rm.reporter_iso3,
    rm.reporter_country_name,
    rm.reporter_region,
    rm.reporter_subregion,
    rm.reporter_continent,
    rm.reporter_is_eu,
    rm.reporter_is_oecd,
    rm.reporter_total_trade_value_usd,
    rm.reporter_food_trade_value_usd,
    rm.reporter_oil_trade_value_usd,
    rm.mom_change_total_trade_pct,
    rm.mom_change_food_trade_pct,
    rm.mom_change_oil_trade_pct
  from macro_spine as ms
  inner join reporter_monthly as rm
    on ms.year_month = rm.year_month
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
  reporter_iso3,
  reporter_country_name,
  reporter_region,
  reporter_subregion,
  reporter_continent,
  reporter_is_eu,
  reporter_is_oecd,
  reporter_total_trade_value_usd,
  reporter_food_trade_value_usd,
  reporter_oil_trade_value_usd,
  mom_change_total_trade_pct,
  mom_change_food_trade_pct,
  mom_change_oil_trade_pct,
  {{ rolling_corr(
    'brent_mom_change',
    'fx_mom_change',
    'currency_view, base_currency_code, fx_currency_code, reporter_iso3',
    'month_start_date',
    6
  ) }} as rolling_6m_corr_brent_fx_mom,
  {{ rolling_corr(
    'brent_mom_change',
    'fx_mom_change',
    'currency_view, base_currency_code, fx_currency_code, reporter_iso3',
    'month_start_date',
    12
  ) }} as rolling_12m_corr_brent_fx_mom,
  {{ rolling_corr(
    'brent_mom_change',
    'mom_change_food_trade_pct',
    'currency_view, base_currency_code, fx_currency_code, reporter_iso3',
    'month_start_date',
    6
  ) }} as rolling_6m_corr_brent_food_trade_mom,
  {{ rolling_corr(
    'brent_mom_change',
    'mom_change_food_trade_pct',
    'currency_view, base_currency_code, fx_currency_code, reporter_iso3',
    'month_start_date',
    12
  ) }} as rolling_12m_corr_brent_food_trade_mom,
  {{ rolling_corr(
    'brent_mom_change',
    'mom_change_oil_trade_pct',
    'currency_view, base_currency_code, fx_currency_code, reporter_iso3',
    'month_start_date',
    6
  ) }} as rolling_6m_corr_brent_oil_trade_mom,
  {{ rolling_corr(
    'brent_mom_change',
    'mom_change_oil_trade_pct',
    'currency_view, base_currency_code, fx_currency_code, reporter_iso3',
    'month_start_date',
    12
  ) }} as rolling_12m_corr_brent_oil_trade_mom
from joined
