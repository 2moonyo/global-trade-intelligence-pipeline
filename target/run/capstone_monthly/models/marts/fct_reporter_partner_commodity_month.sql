
  
    

    create or replace table `capfractal`.`analytics_marts`.`fct_reporter_partner_commodity_month`
      
    
    

    
    OPTIONS()
    as (
      with base as (
  select
    canonical_grain_key,
    reporter_iso3,
    partner_iso3,
    cmd_code,
    period,
    year_month,
    ref_year,
    trade_flow,
    trade_value_usd,
    net_weight_kg,
    gross_weight_kg,
    qty
  from `capfractal`.`analytics_staging`.`stg_comtrade_trade_base`
)

select
  canonical_grain_key,
  reporter_iso3,
  partner_iso3,
  cmd_code,
  period,
  year_month,
  ref_year,
  trade_flow,
  sum(trade_value_usd) as trade_value_usd,
  sum(net_weight_kg) as net_weight_kg,
  sum(gross_weight_kg) as gross_weight_kg,
  sum(qty) as qty,
  case
    when coalesce(sum(net_weight_kg), 0) = 0 then null
    else sum(trade_value_usd) / sum(net_weight_kg)
  end as usd_per_kg,
  count(*) as record_count
from base
group by
  canonical_grain_key,
  reporter_iso3,
  partner_iso3,
  cmd_code,
  period,
  year_month,
  ref_year,
  trade_flow
    );
  