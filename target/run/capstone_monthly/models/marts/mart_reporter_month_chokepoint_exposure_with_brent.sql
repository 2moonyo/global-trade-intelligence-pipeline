
  
    
    

    create  table
      "analytics"."analytics_marts"."mart_reporter_month_chokepoint_exposure_with_brent__dbt_tmp"
  
    as (
      with exposure as (
  select * from "analytics"."analytics_marts"."mart_reporter_month_chokepoint_exposure"
),
brent_long as (
  select *
  from "analytics"."analytics_staging"."stg_brent_monthly"
  where benchmark_code in ('BRENT_EU', 'WTI_US')
),
brent as (
  select
    year_month,
    max(case when benchmark_code = 'BRENT_EU' then avg_price_usd_per_bbl end) as brent_avg_price_usd_per_bbl,
    max(case when benchmark_code = 'BRENT_EU' then month_start_price_usd_per_bbl end) as brent_month_start_price_usd_per_bbl,
    max(case when benchmark_code = 'BRENT_EU' then month_end_price_usd_per_bbl end) as brent_month_end_price_usd_per_bbl,
    max(case when benchmark_code = 'BRENT_EU' then mom_abs_change_usd end) as brent_mom_abs_change_usd,
    max(case when benchmark_code = 'BRENT_EU' then mom_pct_change end) as brent_mom_pct_change,
    max(case when benchmark_code = 'WTI_US' then avg_price_usd_per_bbl end) as wti_avg_price_usd_per_bbl
  from brent_long
  group by 1
)

select
  e.*,
  b.brent_avg_price_usd_per_bbl,
  b.brent_month_start_price_usd_per_bbl,
  b.brent_month_end_price_usd_per_bbl,
  b.brent_mom_abs_change_usd,
  b.brent_mom_pct_change,
  b.wti_avg_price_usd_per_bbl,
  case
    when b.brent_avg_price_usd_per_bbl is not null and b.wti_avg_price_usd_per_bbl is not null
      then b.brent_avg_price_usd_per_bbl - b.wti_avg_price_usd_per_bbl
    else null
  end as brent_wti_spread_usd_per_bbl
from exposure as e
left join brent as b
  on e.year_month = b.year_month
    );
  
  