
  
    
    

    create  table
      "analytics"."analytics_marts"."mart_macro_monthly_features__dbt_tmp"
  
    as (
      -- Grain: one row per year_month + fx_currency_code.
-- Purpose: macro explanatory features for marts; no causal interpretation is implied.

with brent_monthly as (
  select
    year_month,
    avg_price_usd_per_bbl as brent_price_usd,
    mom_pct_change as brent_mom_change
  from "analytics"."analytics_staging"."stg_brent_monthly"
  where benchmark_code = 'BRENT_EU'
),
fx_monthly as (
  select
    year_month,
    fx_currency_code,
    fx_rate_to_usd,
    fx_mom_change
  from "analytics"."analytics_staging"."stg_fx_monthly"
)

select
  fx.year_month,
  brent.brent_price_usd,
  brent.brent_mom_change,
  fx.fx_rate_to_usd,
  fx.fx_mom_change,
  fx.fx_currency_code
from fx_monthly as fx
-- Join on year_month only because Brent is a single benchmark monthly series.
left join brent_monthly as brent
  on fx.year_month = brent.year_month
    );
  
  