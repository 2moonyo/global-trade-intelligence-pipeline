
  
  create view "analytics"."analytics_staging"."stg_comtrade_fact__dbt_tmp" as (
    select
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
from "analytics"."analytics_staging"."stg_comtrade_trade_base"
  );
