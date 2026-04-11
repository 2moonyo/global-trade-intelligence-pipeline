
  
    

    create or replace table `chokepoint-capfractal`.`analytics_marts`.`mart_macro_monthly_features`
      
    
    

    
    OPTIONS()
    as (
      -- Grain: one row per year_month + currency_view + fx_currency_code.
-- Purpose: macro explanatory features for marts; no causal interpretation is implied.

with brent_monthly_long as (
  select
    year_month,
    benchmark_code,
    avg_price_usd_per_bbl,
    mom_pct_change
  from `chokepoint-capfractal`.`analytics_staging`.`stg_brent_monthly`
  where benchmark_code in ('BRENT_EU', 'WTI_US')
),
brent_monthly as (
  select
    year_month,
    max(case when benchmark_code = 'BRENT_EU' then avg_price_usd_per_bbl end) as brent_price_usd,
    max(case when benchmark_code = 'BRENT_EU' then mom_pct_change end) as brent_mom_change,
    max(case when benchmark_code = 'WTI_US' then avg_price_usd_per_bbl end) as wti_price_usd
  from brent_monthly_long
  group by 1
),
fx_monthly as (
  select
    year_month,
    currency_view,
    base_currency_code,
    fx_currency_code,
    fx_rate,
    fx_rate_to_usd,
    fx_mom_change
  from `chokepoint-capfractal`.`analytics_staging`.`stg_fx_monthly`
)

select
  fx.year_month,
  fx.currency_view,
  fx.base_currency_code,
  brent.brent_price_usd,
  brent.brent_mom_change,
  brent.wti_price_usd,
  case
    when brent.brent_price_usd is not null and brent.wti_price_usd is not null
      then brent.brent_price_usd - brent.wti_price_usd
    else null
  end as brent_wti_spread_usd,
  fx.fx_rate,
  fx.fx_rate_to_usd,
  fx.fx_mom_change,
  fx.fx_currency_code
from fx_monthly as fx
-- Join on year_month only because Brent is a single benchmark monthly series.
left join brent_monthly as brent
  on fx.year_month = brent.year_month
    );
  