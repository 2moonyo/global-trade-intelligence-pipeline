
  
  create view "analytics"."analytics_staging"."stg_dim_country__dbt_tmp" as (
    select
  cast(country_code as integer) as country_code,
  upper(trim(iso3)) as iso3,
  country_name,
  region,
  subregion,
  continent,
  cast(is_eu as boolean) as is_eu,
  cast(is_oecd as boolean) as is_oecd
from "analytics"."raw"."dim_country"
  );
