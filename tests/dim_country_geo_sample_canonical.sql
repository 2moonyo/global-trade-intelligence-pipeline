with expected as (
  select 'Belgium' as country_name, 'BEL' as iso3
  union all select 'France', 'FRA'
  union all select 'South Africa', 'ZAF'
  union all select 'China', 'CHN'
),
actual as (
  select
    country_name,
    iso3,
    is_country_map_eligible
  from {{ ref('dim_country') }}
)

select
  e.country_name as expected_country_name,
  e.iso3 as expected_iso3,
  a.country_name as actual_country_name,
  a.iso3 as actual_iso3,
  a.is_country_map_eligible
from expected as e
left join actual as a
  on e.iso3 = a.iso3
where a.iso3 is null
   or a.country_name != e.country_name
   or not coalesce(a.is_country_map_eligible, false)
