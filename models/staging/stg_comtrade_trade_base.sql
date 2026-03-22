with raw_source as (
  select
    cast(ref_date as date) as ref_date,
    cast(period as varchar) as period_raw,
    cast(ref_year as varchar) as ref_year_raw,
    cast(year_month as varchar) as year_month_raw,
    cast(reporter_iso3 as varchar) as reporter_iso3_raw,
    cast(partner_iso3 as varchar) as partner_iso3_raw,
    cast(cmdCode as varchar) as cmd_code_raw,
    cast(cmdDesc as varchar) as commodity_name_raw,
    cast(flowCode as varchar) as flow_code_raw,
    cast(trade_value_usd as double) as trade_value_usd,
    cast(netWgt as double) as net_weight_kg,
    cast(grossWgt as double) as gross_weight_kg,
    cast(qty as double) as qty,
    cast(motCode as integer) as mot_code,
    cast(partner2Code as integer) as partner2_code
  from {{ source('raw', 'comtrade_fact') }}
),
source_data as (
  select
    ref_date,
    try_cast(period_raw as integer) as period,
    case
      when regexp_full_match(year_month_raw, '^\\d{4}-\\d{2}$') then year_month_raw
      when try_cast(period_raw as integer) is not null then substr(period_raw, 1, 4) || '-' || substr(period_raw, 5, 2)
      else null
    end as year_month,
    coalesce(try_cast(ref_year_raw as integer), try_cast(substr(period_raw, 1, 4) as integer)) as ref_year,
    upper(trim(reporter_iso3_raw)) as reporter_iso3,
    upper(trim(partner_iso3_raw)) as partner_iso3,
    trim(cmd_code_raw) as cmd_code,
    trim(commodity_name_raw) as commodity_name_raw,
    case
      when upper(trim(flow_code_raw)) = 'M' then 'Import'
      when upper(trim(flow_code_raw)) = 'X' then 'Export'
      when flow_code_raw is not null then trim(flow_code_raw)
      else null
    end as trade_flow,
    trade_value_usd,
    net_weight_kg,
    gross_weight_kg,
    qty,
    mot_code,
    partner2_code
  from raw_source
)

select
  distinct
  ref_date,
  period,
  year_month,
  ref_year,
  reporter_iso3,
  partner_iso3,
  cmd_code,
  commodity_name_raw,
  trade_flow,
  trade_value_usd,
  net_weight_kg,
  gross_weight_kg,
  qty,
  mot_code,
  partner2_code
from source_data
where period is not null
  and reporter_iso3 is not null
  and partner_iso3 is not null
  and cmd_code is not null
  and year_month is not null
  and trade_flow is not null
