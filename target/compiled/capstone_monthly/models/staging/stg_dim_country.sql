select
  cast(country_code as integer) as country_code,
  upper(trim(iso3)) as iso3,
  country_name,
  region,
  subregion,
  continent,
  cast(is_eu as boolean) as is_eu,
  cast(is_oecd as boolean) as is_oecd
from `fullcap-10111`.`raw`.`dim_country`