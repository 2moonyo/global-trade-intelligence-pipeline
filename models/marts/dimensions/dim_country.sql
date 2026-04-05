with base as (
  select
    country_code,
    iso3,
    country_name,
    region,
    subregion,
    continent,
    is_eu,
    is_oecd
  from {{ ref('stg_dim_country') }}
)

select
  country_code,
  iso3,
  country_name,
  region,
  subregion,
  continent,
  is_eu,
  is_oecd
from base
