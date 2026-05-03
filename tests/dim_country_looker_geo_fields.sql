with expected as (
  select 'USA' as iso3, 'US' as iso2, 'United States' as country_name_looker
  union all select 'RUS', 'RU', 'Russia'
  union all select 'TUR', 'TR', 'Turkey'
  union all select 'VNM', 'VN', 'Vietnam'
  union all select 'KOR', 'KR', 'South Korea'
  union all select 'COD', 'CD', 'Democratic Republic of the Congo'
  union all select 'HKG', 'HK', 'Hong Kong'
  union all select 'MAC', 'MO', 'Macao'
  union all select 'NAM', 'NA', 'Namibia'
),
actual as (
  select
    iso3,
    country_iso2,
    country_name_looker
  from {{ ref('dim_country') }}
)

select
  e.iso3 as expected_iso3,
  e.iso2 as expected_iso2,
  e.country_name_looker as expected_country_name_looker,
  a.country_iso2 as actual_country_iso2,
  a.country_name_looker as actual_country_name_looker
from expected as e
left join actual as a
  on e.iso3 = a.iso3
where a.iso3 is null
   or a.country_iso2 != e.iso2
   or a.country_name_looker != e.country_name_looker
