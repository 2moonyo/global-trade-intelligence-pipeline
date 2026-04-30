-- Dashboard support mart for monthly bloc comparisons.
-- Grain: one row per bloc_code + month_start_date.
-- Important: rows are not additive across bloc_code because countries may belong to multiple blocs.

with bloc_membership as (
  select
    iso3,
    bloc_code,
    bloc_name,
    bloc_type
  from {{ ref('dim_country_bloc_membership') }}
),
member_counts as (
  select
    bloc_code,
    count(distinct iso3) as member_country_count
  from bloc_membership
  group by 1
),
reporter_commodity_month as (
  select
    {{ canonical_country_iso3('rcm.reporter_iso3') }} as reporter_iso3,
    rcm.cmd_code,
    rcm.period,
    rcm.year_month,
    rcm.month_start_date,
    rcm.year,
    rcm.month,
    rcm.total_trade_value_usd,
    rcm.import_trade_value_usd,
    rcm.export_trade_value_usd,
    rcm.food_flag,
    rcm.energy_flag,
    co.hs4,
    co.hs6
  from {{ ref('mart_reporter_commodity_month_trade_summary') }} as rcm
  left join {{ ref('dim_commodity') }} as co
    on rcm.cmd_code = co.cmd_code
),
bloc_trade as (
  select
    bm.bloc_code,
    max(bm.bloc_name) as bloc_name,
    max(bm.bloc_type) as bloc_type,
    rcm.month_start_date,
    max(rcm.year_month) as year_month,
    max(rcm.year) as year,
    max(rcm.month) as month,
    sum(rcm.total_trade_value_usd) as bloc_total_trade_value_usd,
    sum(rcm.import_trade_value_usd) as bloc_import_trade_value_usd,
    sum(rcm.export_trade_value_usd) as bloc_export_trade_value_usd,
    sum(case when coalesce(rcm.food_flag, false) then rcm.total_trade_value_usd else 0 end) as bloc_food_trade_value_usd,
    sum(case when coalesce(rcm.food_flag, false) then rcm.import_trade_value_usd else 0 end) as bloc_food_import_value_usd,
    sum(case when coalesce(rcm.food_flag, false) then rcm.export_trade_value_usd else 0 end) as bloc_food_export_value_usd,
    sum(case when coalesce(rcm.energy_flag, false) then rcm.total_trade_value_usd else 0 end) as bloc_energy_trade_value_usd,
    sum(case when coalesce(rcm.energy_flag, false) then rcm.import_trade_value_usd else 0 end) as bloc_energy_import_value_usd,
    sum(case when coalesce(rcm.energy_flag, false) then rcm.export_trade_value_usd else 0 end) as bloc_energy_export_value_usd,
    sum(
      case
        when rcm.hs4 in ('2709', '2710')
          or rcm.hs6 = '271111'
          or cast(rcm.cmd_code as {{ dbt.type_string() }}) in ('2709', '2710', '271111')
          then rcm.total_trade_value_usd
        else 0
      end
    ) as bloc_oil_trade_value_usd,
    sum(
      case
        when rcm.hs4 in ('2709', '2710')
          or rcm.hs6 = '271111'
          or cast(rcm.cmd_code as {{ dbt.type_string() }}) in ('2709', '2710', '271111')
          then rcm.import_trade_value_usd
        else 0
      end
    ) as bloc_oil_import_value_usd,
    sum(
      case
        when rcm.hs4 in ('2709', '2710')
          or rcm.hs6 = '271111'
          or cast(rcm.cmd_code as {{ dbt.type_string() }}) in ('2709', '2710', '271111')
          then rcm.export_trade_value_usd
        else 0
      end
    ) as bloc_oil_export_value_usd,
    count(distinct rcm.reporter_iso3) as reporting_country_count
  from reporter_commodity_month as rcm
  inner join bloc_membership as bm
    on rcm.reporter_iso3 = bm.iso3
  group by 1, 4
),
brent_monthly as (
  select
    year_month,
    max(case when benchmark_code = 'BRENT_EU' then avg_price_usd_per_bbl end) as brent_price_usd,
    max(case when benchmark_code = 'BRENT_EU' then mom_pct_change end) as brent_mom_change,
    max(case when benchmark_code = 'WTI_US' then avg_price_usd_per_bbl end) as wti_price_usd
  from {{ ref('stg_brent_monthly') }}
  where benchmark_code in ('BRENT_EU', 'WTI_US')
  group by 1
),
global_bounds as (
  select max(month_start_date) as latest_month_start_date
  from bloc_trade
),
with_macro as (
  select
    bt.bloc_code,
    bt.bloc_name,
    bt.bloc_type,
    bt.month_start_date,
    bt.year_month,
    bt.year,
    bt.month,
    mc.member_country_count,
    bt.reporting_country_count,
    bt.bloc_total_trade_value_usd,
    bt.bloc_import_trade_value_usd,
    bt.bloc_export_trade_value_usd,
    bt.bloc_food_trade_value_usd,
    bt.bloc_food_import_value_usd,
    bt.bloc_food_export_value_usd,
    bt.bloc_energy_trade_value_usd,
    bt.bloc_energy_import_value_usd,
    bt.bloc_energy_export_value_usd,
    bt.bloc_oil_trade_value_usd,
    bt.bloc_oil_import_value_usd,
    bt.bloc_oil_export_value_usd,
    brent.brent_price_usd,
    brent.brent_mom_change,
    brent.wti_price_usd,
    case
      when brent.brent_price_usd is not null and brent.wti_price_usd is not null
        then brent.brent_price_usd - brent.wti_price_usd
      else null
    end as brent_wti_spread_usd
  from bloc_trade as bt
  left join member_counts as mc
    on bt.bloc_code = mc.bloc_code
  left join brent_monthly as brent
    on bt.year_month = brent.year_month
),
with_previous as (
  select
    wm.*,
    lag(month_start_date, 1) over (
      partition by bloc_code
      order by month_start_date
    ) as previous_month_start_date,
    lag(bloc_total_trade_value_usd, 1) over (
      partition by bloc_code
      order by month_start_date
    ) as previous_total_trade_value_usd,
    lag(bloc_food_trade_value_usd, 1) over (
      partition by bloc_code
      order by month_start_date
    ) as previous_food_trade_value_usd,
    lag(bloc_energy_trade_value_usd, 1) over (
      partition by bloc_code
      order by month_start_date
    ) as previous_energy_trade_value_usd,
    lag(bloc_oil_trade_value_usd, 1) over (
      partition by bloc_code
      order by month_start_date
    ) as previous_oil_trade_value_usd
  from with_macro as wm
)

select
  wp.bloc_code,
  wp.bloc_name,
  wp.bloc_type,
  wp.month_start_date,
  wp.year_month,
  wp.year,
  wp.month,
  wp.member_country_count,
  wp.reporting_country_count,
  wp.bloc_total_trade_value_usd,
  wp.bloc_import_trade_value_usd,
  wp.bloc_export_trade_value_usd,
  wp.bloc_food_trade_value_usd,
  wp.bloc_food_import_value_usd,
  wp.bloc_food_export_value_usd,
  wp.bloc_energy_trade_value_usd,
  wp.bloc_energy_import_value_usd,
  wp.bloc_energy_export_value_usd,
  wp.bloc_oil_trade_value_usd,
  wp.bloc_oil_import_value_usd,
  wp.bloc_oil_export_value_usd,
  wp.brent_price_usd,
  wp.brent_mom_change,
  wp.wti_price_usd,
  wp.brent_wti_spread_usd,
  {{ safe_divide('wp.bloc_food_trade_value_usd', 'wp.bloc_total_trade_value_usd') }} as food_share_of_bloc_trade_pct,
  {{ safe_divide('wp.bloc_energy_trade_value_usd', 'wp.bloc_total_trade_value_usd') }} as energy_share_of_bloc_trade_pct,
  {{ safe_divide('wp.bloc_oil_trade_value_usd', 'wp.bloc_total_trade_value_usd') }} as oil_share_of_bloc_trade_pct,
  {{ safe_divide('wp.bloc_import_trade_value_usd', 'wp.bloc_total_trade_value_usd') }} as import_dependency_proxy_pct,
  {{ safe_divide('wp.bloc_export_trade_value_usd', 'wp.bloc_total_trade_value_usd') }} as export_orientation_proxy_pct,
  case
    when wp.previous_month_start_date is not null
      and {{ date_add_months('wp.previous_month_start_date', 1) }} = wp.month_start_date
      then {{ safe_divide(
        'wp.bloc_total_trade_value_usd - wp.previous_total_trade_value_usd',
        'wp.previous_total_trade_value_usd'
      ) }}
    else null
  end as mom_change_total_trade_pct,
  case
    when wp.previous_month_start_date is not null
      and {{ date_add_months('wp.previous_month_start_date', 1) }} = wp.month_start_date
      then {{ safe_divide(
        'wp.bloc_food_trade_value_usd - wp.previous_food_trade_value_usd',
        'wp.previous_food_trade_value_usd'
      ) }}
    else null
  end as mom_change_food_trade_pct,
  case
    when wp.previous_month_start_date is not null
      and {{ date_add_months('wp.previous_month_start_date', 1) }} = wp.month_start_date
      then {{ safe_divide(
        'wp.bloc_energy_trade_value_usd - wp.previous_energy_trade_value_usd',
        'wp.previous_energy_trade_value_usd'
      ) }}
    else null
  end as mom_change_energy_trade_pct,
  case
    when wp.previous_month_start_date is not null
      and {{ date_add_months('wp.previous_month_start_date', 1) }} = wp.month_start_date
      then {{ safe_divide(
        'wp.bloc_oil_trade_value_usd - wp.previous_oil_trade_value_usd',
        'wp.previous_oil_trade_value_usd'
      ) }}
    else null
  end as mom_change_oil_trade_pct,
  case when wp.month_start_date = gb.latest_month_start_date then true else false end as latest_month_flag
from with_previous as wp
cross join global_bounds as gb
