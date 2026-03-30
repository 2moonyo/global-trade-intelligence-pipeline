

  create or replace view `capfractal`.`analytics_staging`.`stg_dim_country`
  OPTIONS()
  as select
  cast(country_code as integer) as country_code,
  upper(trim(iso3)) as iso3,
  country_name,
  region,
  subregion,
  continent,
  cast(is_eu as boolean) as is_eu,
  cast(is_oecd as boolean) as is_oecd
from `capfractal`.`raw`.`dim_country`;

