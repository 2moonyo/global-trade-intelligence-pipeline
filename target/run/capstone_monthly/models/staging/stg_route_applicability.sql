
  
  create view "analytics"."analytics_staging"."stg_route_applicability__dbt_tmp" as (
    select
  upper(trim(reporter_iso3)) as reporter_iso3,
  upper(trim(partner_iso3)) as partner_iso3,
  upper(trim(partner2_iso3)) as partner2_iso3,
  cast(row_count as bigint) as row_count,
  cast(trade_value_usd as double) as trade_value_usd,
  cast(has_sea as boolean) as has_sea,
  cast(has_inland_water as boolean) as has_inland_water,
  cast(has_unknown as boolean) as has_unknown,
  cast(has_non_marine as boolean) as has_non_marine,
  mot_codes_seen,
  route_applicability_status
from "analytics"."raw"."route_applicability"
  );
