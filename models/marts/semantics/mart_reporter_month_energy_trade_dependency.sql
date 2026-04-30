-- Country-level monthly dependency mart for structural trade and energy vulnerability.
-- Grain: one row per reporter_iso3 + month_start_date.
-- Electricity-source proxy fields are descriptive analytical proxies and do not represent total final energy consumption.

with base as (
  select
    reporter_iso3,
    reporter_name,
    reporter_region,
    reporter_subregion,
    reporter_continent,
    reporter_is_eu,
    reporter_is_oecd,
    period,
    year_month,
    month_start_date,
    month_label,
    year,
    month,
    quarter,
    total_trade_value_usd,
    total_import_value_usd,
    total_export_value_usd,
    energy_import_pct,
    renewable_share_pct,
    fossil_share_pct,
    oil_electricity_share_pct,
    gas_electricity_share_pct,
    coal_electricity_share_pct,
    chokepoint_exposure_pct,
    supplier_concentration_pct,
    structural_risk_score,
    latest_month_flag
  from {{ ref('mart_reporter_structural_vulnerability') }}
),
trade_rollup as (
  select
    {{ canonical_country_iso3('rcm.reporter_iso3') }} as reporter_iso3,
    rcm.month_start_date,
    max(rcm.year_month) as year_month,
    sum(case when coalesce(rcm.food_flag, false) then rcm.total_trade_value_usd else 0 end) as food_trade_value_usd,
    sum(case when coalesce(rcm.food_flag, false) then rcm.import_trade_value_usd else 0 end) as food_import_value_usd,
    sum(case when coalesce(rcm.food_flag, false) then rcm.export_trade_value_usd else 0 end) as food_export_value_usd,
    sum(case when coalesce(rcm.energy_flag, false) then rcm.total_trade_value_usd else 0 end) as energy_trade_value_usd,
    sum(case when coalesce(rcm.energy_flag, false) then rcm.import_trade_value_usd else 0 end) as energy_import_trade_value_usd,
    sum(case when coalesce(rcm.energy_flag, false) then rcm.export_trade_value_usd else 0 end) as energy_export_trade_value_usd,
    sum(
      case
        when co.hs4 in ('2709', '2710')
          or co.hs6 = '271111'
          or cast(rcm.cmd_code as {{ dbt.type_string() }}) in ('2709', '2710', '271111')
          then rcm.total_trade_value_usd
        else 0
      end
    ) as oil_trade_value_usd,
    sum(
      case
        when co.hs4 in ('2709', '2710')
          or co.hs6 = '271111'
          or cast(rcm.cmd_code as {{ dbt.type_string() }}) in ('2709', '2710', '271111')
          then rcm.import_trade_value_usd
        else 0
      end
    ) as oil_import_trade_value_usd,
    sum(
      case
        when co.hs4 in ('2709', '2710')
          or co.hs6 = '271111'
          or cast(rcm.cmd_code as {{ dbt.type_string() }}) in ('2709', '2710', '271111')
          then rcm.export_trade_value_usd
        else 0
      end
    ) as oil_export_trade_value_usd
  from {{ ref('mart_reporter_commodity_month_trade_summary') }} as rcm
  left join {{ ref('dim_commodity') }} as co
    on rcm.cmd_code = co.cmd_code
  group by 1, 2
),
brent_monthly as (
  select
    year_month,
    max(case when benchmark_code = 'BRENT_EU' then avg_price_usd_per_bbl end) as brent_price_usd
  from {{ ref('stg_brent_monthly') }}
  where benchmark_code in ('BRENT_EU', 'WTI_US')
  group by 1
),
country_map as (
  select
    iso3,
    is_country_map_eligible
  from {{ ref('dim_country') }}
),
assembled as (
  select
    b.reporter_iso3,
    b.reporter_name,
    b.reporter_region,
    b.reporter_subregion,
    b.reporter_continent,
    b.reporter_is_eu,
    b.reporter_is_oecd,
    b.period,
    b.year_month,
    b.month_start_date,
    b.month_label,
    b.year,
    b.month,
    b.quarter,
    b.total_trade_value_usd,
    b.total_import_value_usd,
    b.total_export_value_usd,
    b.energy_import_pct,
    b.renewable_share_pct,
    b.fossil_share_pct,
    b.oil_electricity_share_pct,
    b.gas_electricity_share_pct,
    b.coal_electricity_share_pct,
    least(
      coalesce(b.oil_electricity_share_pct, 0)
      + coalesce(b.gas_electricity_share_pct, 0)
      + coalesce(b.coal_electricity_share_pct, 0),
      100
    ) as fossil_or_thermal_power_proxy_pct,
    tr.food_trade_value_usd,
    tr.food_import_value_usd,
    tr.food_export_value_usd,
    tr.energy_trade_value_usd,
    tr.energy_import_trade_value_usd,
    tr.energy_export_trade_value_usd,
    tr.oil_trade_value_usd,
    tr.oil_import_trade_value_usd,
    tr.oil_export_trade_value_usd,
    b.chokepoint_exposure_pct,
    b.supplier_concentration_pct,
    b.structural_risk_score,
    brent.brent_price_usd,
    coalesce(cm.is_country_map_eligible, false) as is_country_map_eligible,
    b.latest_month_flag
  from base as b
  left join trade_rollup as tr
    on b.reporter_iso3 = tr.reporter_iso3
   and b.month_start_date = tr.month_start_date
  left join brent_monthly as brent
    on b.year_month = brent.year_month
  left join country_map as cm
    on b.reporter_iso3 = cm.iso3
),
with_shares as (
  select
    *,
    {{ safe_divide('energy_trade_value_usd', 'total_trade_value_usd') }} as energy_trade_share_pct,
    {{ safe_divide('oil_trade_value_usd', 'total_trade_value_usd') }} as oil_trade_share_pct,
    {{ safe_divide('food_trade_value_usd', 'total_trade_value_usd') }} as food_trade_share_pct,
    {{ safe_divide('food_import_value_usd', 'total_import_value_usd') }} as food_import_share_pct,
    {{ safe_divide('oil_import_trade_value_usd', 'total_import_value_usd') }} as oil_import_share_pct,
    coalesce(fossil_share_pct, fossil_or_thermal_power_proxy_pct) as fossil_dependency_axis_pct
  from assembled
),
with_thresholds as (
  select
    *,
    percentile_cont(energy_trade_share_pct, 0.5) over (
      partition by month_start_date
    ) as energy_trade_share_median_in_month,
    percentile_cont(fossil_dependency_axis_pct, 0.5) over (
      partition by month_start_date
    ) as fossil_dependency_median_in_month
  from with_shares
)

select
  reporter_iso3,
  reporter_name,
  reporter_region,
  reporter_subregion,
  reporter_continent,
  reporter_is_eu,
  reporter_is_oecd,
  period,
  year_month,
  month_start_date,
  month_label,
  year,
  month,
  quarter,
  total_trade_value_usd,
  total_import_value_usd,
  total_export_value_usd,
  energy_import_pct,
  renewable_share_pct,
  fossil_share_pct,
  oil_electricity_share_pct,
  gas_electricity_share_pct,
  coal_electricity_share_pct,
  fossil_or_thermal_power_proxy_pct,
  food_trade_value_usd,
  food_import_value_usd,
  food_export_value_usd,
  energy_trade_value_usd,
  energy_import_trade_value_usd,
  energy_export_trade_value_usd,
  oil_trade_value_usd,
  oil_import_trade_value_usd,
  oil_export_trade_value_usd,
  chokepoint_exposure_pct,
  supplier_concentration_pct,
  structural_risk_score,
  brent_price_usd,
  energy_trade_share_pct,
  oil_trade_share_pct,
  food_trade_share_pct,
  food_import_share_pct,
  oil_import_share_pct,
  case
    when coalesce(fossil_dependency_axis_pct, 0) >= coalesce(fossil_dependency_median_in_month, 0)
      and coalesce(energy_trade_share_pct, 0) >= coalesce(energy_trade_share_median_in_month, 0)
      then 'High fossil / High trade'
    when coalesce(fossil_dependency_axis_pct, 0) >= coalesce(fossil_dependency_median_in_month, 0)
      and coalesce(energy_trade_share_pct, 0) < coalesce(energy_trade_share_median_in_month, 0)
      then 'High fossil / Low trade'
    when coalesce(fossil_dependency_axis_pct, 0) < coalesce(fossil_dependency_median_in_month, 0)
      and coalesce(energy_trade_share_pct, 0) >= coalesce(energy_trade_share_median_in_month, 0)
      then 'Low fossil / High trade'
    else 'Low fossil / Low trade'
  end as fossil_dependency_vs_trade_quadrant,
  concat(
    reporter_name,
    ' | ',
    case
      when coalesce(fossil_dependency_axis_pct, 0) >= coalesce(fossil_dependency_median_in_month, 0)
        and coalesce(energy_trade_share_pct, 0) >= coalesce(energy_trade_share_median_in_month, 0)
        then 'High fossil / High trade'
      when coalesce(fossil_dependency_axis_pct, 0) >= coalesce(fossil_dependency_median_in_month, 0)
        and coalesce(energy_trade_share_pct, 0) < coalesce(energy_trade_share_median_in_month, 0)
        then 'High fossil / Low trade'
      when coalesce(fossil_dependency_axis_pct, 0) < coalesce(fossil_dependency_median_in_month, 0)
        and coalesce(energy_trade_share_pct, 0) >= coalesce(energy_trade_share_median_in_month, 0)
        then 'Low fossil / High trade'
      else 'Low fossil / Low trade'
    end,
    ' | risk ',
    cast(cast(round(coalesce(structural_risk_score, 0), 0) as {{ dbt.type_int() }}) as {{ dbt.type_string() }})
  ) as vulnerability_story_label,
  reporter_name as looker_country_name,
  reporter_iso3 as looker_country_code,
  reporter_name as map_location_label,
  is_country_map_eligible,
  latest_month_flag
from with_thresholds
