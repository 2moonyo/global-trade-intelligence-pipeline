-- Grain: one row per year_month + currency_view + fx_currency_code.
-- Builds EUR-base canonical rates from the ECB-native monthly pair table plus USD-base derived crosses via EUR/USD.

with raw_fx as (
  select
    year_month,
    cast(month_start_date as date) as month_start_date,
    cast(year as INT64) as year,
    cast(month as INT64) as month,
    upper(trim(base_currency_code)) as base_currency_code,
    upper(trim(quote_currency_code)) as quote_currency_code,
    cast(fx_rate as FLOAT64) as fx_rate
  from `capfractal`.`raw`.`ecb_fx_eu_monthly`
  where year_month is not null
    and 
    regexp_contains(cast(year_month as string), r'^\d{4}-\d{2}$')
  
    and base_currency_code is not null
    and quote_currency_code is not null
    and fx_rate is not null
),
eur_usd_bridge as (
  select
    year_month,
    month_start_date,
    year,
    month,
    fx_rate as eur_usd_rate
  from raw_fx
  where base_currency_code = 'EUR'
    and quote_currency_code = 'USD'
),
eur_base_direct as (
  select
    raw.year_month,
    raw.month_start_date,
    raw.year,
    raw.month,
    'EUR_base' as currency_view,
    'EUR' as base_currency_code,
    raw.quote_currency_code as fx_currency_code,
    raw.fx_rate,
    case
      when raw.quote_currency_code = 'USD' then 1.0
      else case
    when raw.fx_rate is null or raw.fx_rate = 0 then null
    else bridge.eur_usd_rate / raw.fx_rate
  end
    end as fx_rate_to_usd
  from raw_fx as raw
  inner join eur_usd_bridge as bridge
    on raw.year_month = bridge.year_month
  where raw.base_currency_code = 'EUR'
    and raw.quote_currency_code <> 'EUR'
),
usd_eur_bridge as (
  select
    bridge.year_month,
    bridge.month_start_date,
    bridge.year,
    bridge.month,
    'USD_base' as currency_view,
    'USD' as base_currency_code,
    'EUR' as fx_currency_code,
    case
    when bridge.eur_usd_rate is null or bridge.eur_usd_rate = 0 then null
    else 1.0 / bridge.eur_usd_rate
  end as fx_rate,
    bridge.eur_usd_rate as fx_rate_to_usd
  from eur_usd_bridge as bridge
),
usd_base_derived as (
  select
    eur.year_month,
    eur.month_start_date,
    eur.year,
    eur.month,
    'USD_base' as currency_view,
    'USD' as base_currency_code,
    eur.fx_currency_code,
    case
    when bridge.eur_usd_rate is null or bridge.eur_usd_rate = 0 then null
    else eur.fx_rate / bridge.eur_usd_rate
  end as fx_rate,
    eur.fx_rate_to_usd
  from eur_base_direct as eur
  inner join eur_usd_bridge as bridge
    on eur.year_month = bridge.year_month
  where eur.fx_currency_code <> 'USD'
),
combined as (
  select * from eur_base_direct
  union all
  select * from usd_eur_bridge
  union all
  select * from usd_base_derived
),
with_momentum as (
  select
    year_month,
    month_start_date,
    year,
    month,
    currency_view,
    base_currency_code,
    fx_currency_code,
    fx_rate,
    fx_rate_to_usd,
    lag(fx_rate) over (
      partition by currency_view, base_currency_code, fx_currency_code
      order by year_month
    ) as prev_fx_rate
  from combined
)

select
  year_month,
  month_start_date,
  year,
  month,
  currency_view,
  base_currency_code,
  fx_currency_code,
  fx_rate,
  fx_rate_to_usd,
  case
    when prev_fx_rate is null or prev_fx_rate = 0 then null
    else (fx_rate - prev_fx_rate) / prev_fx_rate
  end as fx_mom_change
from with_momentum