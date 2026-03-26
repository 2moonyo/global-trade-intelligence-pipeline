-- Grain: one row per year_month + fx_currency_code.
-- Converts ECB quote-per-EUR rates into USD-per-currency and computes monthly momentum.

with raw_fx as (
  select
    cast(date as date) as fx_date,
    upper(trim(quote_ccy)) as quote_ccy,
    upper(trim(base_ccy)) as base_ccy,
    cast(rate as double) as quote_per_base_rate,
    load_ts
  from "analytics"."raw"."ecb_fx_eu_daily"
  where date is not null
    and quote_ccy is not null
    and base_ccy is not null
    and rate is not null
),
latest_daily as (
  select
    fx_date,
    quote_ccy,
    base_ccy,
    quote_per_base_rate,
    load_ts,
    row_number() over (
      partition by fx_date, quote_ccy, base_ccy
      order by load_ts desc
    ) as _rn
  from raw_fx
),
daily_clean as (
  select
    fx_date,
    quote_ccy,
    base_ccy,
    quote_per_base_rate
  from latest_daily
  where _rn = 1
),
daily_with_usd_per_base as (
  select
    fx_date,
    quote_ccy,
    base_ccy,
    quote_per_base_rate,
    max(
      case
        when quote_ccy = 'USD' then quote_per_base_rate
        else null
      end
    ) over (partition by fx_date, base_ccy) as usd_per_base_rate
  from daily_clean
),
daily_usd_converted as (
  select
    strftime(fx_date, '%Y-%m') as year_month,
    quote_ccy as fx_currency_code,
    case
      when quote_ccy = 'USD' then 1.0
      when usd_per_base_rate is null or quote_per_base_rate = 0 then null
      else usd_per_base_rate / quote_per_base_rate
    end as fx_rate_to_usd
  from daily_with_usd_per_base
),
monthly_rates as (
  select
    year_month,
    fx_currency_code,
    avg(fx_rate_to_usd) as fx_rate_to_usd
  from daily_usd_converted
  where fx_rate_to_usd is not null
  group by 1, 2
),
monthly_with_prev as (
  select
    year_month,
    fx_currency_code,
    fx_rate_to_usd,
    lag(fx_rate_to_usd) over (
      partition by fx_currency_code
      order by year_month
    ) as prev_month_rate_to_usd
  from monthly_rates
)

select
  year_month,
  fx_currency_code,
  fx_rate_to_usd,
  case
    when prev_month_rate_to_usd is null or prev_month_rate_to_usd = 0 then null
    else (fx_rate_to_usd - prev_month_rate_to_usd) / prev_month_rate_to_usd
  end as fx_mom_change
from monthly_with_prev