with base as (
  select
    country_code,
    iso3,
    country_name,
    region,
    subregion,
    continent,
    is_eu,
    is_oecd,
    is_country_group,
    is_country_map_eligible
  from {{ ref('stg_dim_country') }}
  where iso3 is not null
),
ranked as (
  select
    base.*,
    row_number() over (
      partition by iso3
      order by
        case when is_country_map_eligible then 0 else 1 end,
        case when country_code is null then 1 else 0 end,
        country_code
    ) as country_rank
  from base
)

select
  country_code,
  iso3,
  country_name,
  region,
  subregion,
  continent,
  is_eu,
  is_oecd,
  is_country_group,
  is_country_map_eligible
from ranked
where country_rank = 1
