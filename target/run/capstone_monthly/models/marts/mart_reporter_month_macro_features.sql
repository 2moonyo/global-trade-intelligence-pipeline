
  
    
    

    create  table
      "analytics"."analytics_marts"."mart_reporter_month_macro_features__dbt_tmp"
  
    as (
      -- Grain: one row per reporter_iso3 + period + fx_currency_code.
-- Purpose: reporter-month macro context combining Brent, FX, and annual energy indicators.

with reporter_month as (
  select
    reporter_iso3,
    reporter_country_name,
    reporter_region,
    reporter_subregion,
    reporter_continent,
    reporter_is_eu,
    reporter_is_oecd,
    period,
    year_month,
    year,
    month,
    quarter,
    month_start_date
  from "analytics"."analytics_marts"."mart_reporter_month_trade_summary"
),
macro_monthly as (
  select
    year_month,
    fx_currency_code,
    brent_price_usd,
    brent_mom_change,
    fx_rate_to_usd,
    fx_mom_change
  from "analytics"."analytics_marts"."mart_macro_monthly_features"
),
energy_annual_pivot as (
  select
    reporter_iso3,
    year,
    max(case when indicator_code = 'renewables_share' then indicator_value end) as renewables_share,
    max(case when indicator_code = 'fossil_fuels_share' then indicator_value end) as fossil_fuels_share,
    max(case when indicator_code = 'dependency_on_imported_energy' then indicator_value end) as dependency_on_imported_energy,
    max(case when indicator_code = 'oil_electricity_share' then indicator_value end) as oil_electricity_share,
    max(case when indicator_code = 'gas_electricity_share' then indicator_value end) as gas_electricity_share,
    max(case when indicator_code = 'coal_electricity_share' then indicator_value end) as coal_electricity_share
  from "analytics"."analytics_marts"."mart_reporter_energy_vulnerability"
  group by 1, 2
)

select
  rm.reporter_iso3,
  rm.reporter_country_name,
  rm.reporter_region,
  rm.reporter_subregion,
  rm.reporter_continent,
  rm.reporter_is_eu,
  rm.reporter_is_oecd,
  rm.period,
  rm.year_month,
  rm.year,
  rm.month,
  rm.quarter,
  rm.month_start_date,
  mm.fx_currency_code,
  mm.brent_price_usd,
  mm.brent_mom_change,
  mm.fx_rate_to_usd,
  mm.fx_mom_change,
  eap.renewables_share,
  eap.fossil_fuels_share,
  eap.dependency_on_imported_energy,
  eap.oil_electricity_share,
  eap.gas_electricity_share,
  eap.coal_electricity_share,
  'ANNUAL_BROADCAST_TO_MONTH_BY_YEAR' as energy_join_method
from reporter_month as rm
-- Join monthly macro series on month key.
left join macro_monthly as mm
  on rm.year_month = mm.year_month
-- Annual energy indicators are deliberately broadcast across months by matching on reporter + year.
left join energy_annual_pivot as eap
  on rm.reporter_iso3 = eap.reporter_iso3
 and rm.year = eap.year
    );
  
  