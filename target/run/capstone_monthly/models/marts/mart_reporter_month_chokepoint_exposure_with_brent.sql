
  
    
    

    create  table
      "analytics"."analytics_marts"."mart_reporter_month_chokepoint_exposure_with_brent__dbt_tmp"
  
    as (
      with exposure as (
  select * from "analytics"."analytics_marts"."mart_reporter_month_chokepoint_exposure"
),
brent as (
  select *
  from "analytics"."analytics_staging"."stg_brent_monthly"
  where benchmark_code = 'BRENT_EU'
)

select
  e.*,
  b.avg_price_usd_per_bbl as brent_avg_price_usd_per_bbl,
  b.month_start_price_usd_per_bbl as brent_month_start_price_usd_per_bbl,
  b.month_end_price_usd_per_bbl as brent_month_end_price_usd_per_bbl,
  b.mom_abs_change_usd as brent_mom_abs_change_usd,
  b.mom_pct_change as brent_mom_pct_change
from exposure as e
left join brent as b
  on e.year_month = b.year_month
    );
  
  