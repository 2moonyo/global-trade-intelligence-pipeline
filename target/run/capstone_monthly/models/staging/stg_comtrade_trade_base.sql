

  create or replace view `capfractal`.`analytics_staging`.`stg_comtrade_trade_base`
  OPTIONS()
  as with raw_source as (
  select
    cast(ref_date as date) as ref_date,
    cast(period as string) as period_raw,
    cast(ref_year as string) as ref_year_raw,
    cast(year_month as string) as year_month_raw,
    cast(reporter_iso3 as string) as reporter_iso3_raw,
    cast(partner_iso3 as string) as partner_iso3_raw,
    cast(cmdCode as string) as cmd_code_raw,
    cast(cmdDesc as string) as commodity_name_raw,
    cast(flowCode as string) as flow_code_raw,
    cast(trade_value_usd as FLOAT64) as trade_value_usd,
    cast(netWgt as FLOAT64) as net_weight_kg,
    cast(grossWgt as FLOAT64) as gross_weight_kg,
    cast(qty as FLOAT64) as qty,
    cast(motCode as integer) as mot_code,
    cast(partner2Code as integer) as partner2_code
  from `capfractal`.`raw`.`comtrade_fact`
),
source_data as (
  select
    ref_date,
    
    safe_cast(period_raw as INT64)
   as period,
    case
      when 
    regexp_contains(cast(year_month_raw as string), r'^\d{4}-\d{2}$')
   then year_month_raw
      when 
    safe_cast(period_raw as INT64)
   is not null then substr(period_raw, 1, 4) || '-' || substr(period_raw, 5, 2)
      else null
    end as year_month,
    coalesce(
      
    safe_cast(ref_year_raw as INT64)
  ,
      
    safe_cast(substr(period_raw, 1, 4) as INT64)
  
    ) as ref_year,
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
  
    to_hex(md5(cast(coalesce(reporter_iso3, '') || '|' || coalesce(partner_iso3, '') || '|' || coalesce(cmd_code, '') || '|' || cast(period as string) || '|' || coalesce(trade_flow, '') as string)))
   as canonical_grain_key,
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
  and trade_flow is not null;

