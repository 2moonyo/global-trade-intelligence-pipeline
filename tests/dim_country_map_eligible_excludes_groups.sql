select
  country_code,
  iso3,
  country_name,
  is_country_group,
  is_country_map_eligible
from {{ ref('dim_country') }}
where coalesce(is_country_map_eligible, false)
  and (
    coalesce(is_country_group, false)
    or country_code = 0
    or iso3 in ('EU', 'EUR', 'WLD', 'W00', 'A79', 'E19', 'F19', 'S19', 'X1', 'XX', '_X')
    or lower(country_name) in ('european union', 'world')
  )
