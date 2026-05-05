-- Support dimension for dashboard bloc comparisons.
-- Grain: one row per iso3 + bloc_code.

with seeded_membership as (
  select
    {{ canonical_country_iso3('iso3') }} as iso3,
    upper(trim(bloc_code)) as bloc_code,
    trim(bloc_name) as bloc_name,
    trim(bloc_type) as bloc_type,
    trim(membership_status) as membership_status,
    trim(notes) as notes
  from {{ ref('dim_country_bloc_membership_seed') }}
),
eligible_countries as (
  select
    iso3,
    country_name,
    region,
    subregion,
    continent,
    is_eu,
    is_oecd,
    is_country_map_eligible,
    is_country_group
  from {{ ref('dim_country') }}
  where iso3 is not null
),
derived_eu as (
  select
    iso3,
    'EU' as bloc_code,
    'European Union' as bloc_name,
    'derived_institutional_bloc' as bloc_type,
    'member' as membership_status,
    'Derived from dim_country.is_eu for dashboard comparison symmetry.' as notes
  from eligible_countries
  where coalesce(is_eu, false)
    and not coalesce(is_country_group, false)
),
derived_oecd as (
  select
    iso3,
    'OECD' as bloc_code,
    'OECD' as bloc_name,
    'derived_institutional_bloc' as bloc_type,
    'member' as membership_status,
    'Derived from dim_country.is_oecd for dashboard comparison symmetry.' as notes
  from eligible_countries
  where coalesce(is_oecd, false)
    and not coalesce(is_country_group, false)
),
western_aligned_proxy as (
  select distinct
    iso3,
    'WESTERN_ALIGNED_PROXY' as bloc_code,
    'Western aligned proxy' as bloc_name,
    'analytical_proxy' as bloc_type,
    'proxy_member' as membership_status,
    'Analytical grouping for dashboard comparison only; not an official legal or political classification.' as notes
  from (
    select iso3 from seeded_membership where bloc_code = 'G7'
    union distinct
    select iso3 from derived_eu
    union distinct
    select iso3 from derived_oecd
  ) as grouped
),
all_membership as (
  select * from seeded_membership
  union distinct
  select * from derived_eu
  union distinct
  select * from derived_oecd
  union distinct
  select * from western_aligned_proxy
)

select
  m.iso3,
  m.bloc_code,
  m.bloc_name,
  m.bloc_type,
  m.membership_status,
  m.notes,
  c.country_name,
  c.region,
  c.subregion,
  c.continent,
  c.is_eu,
  c.is_oecd,
  c.is_country_map_eligible
from all_membership as m
left join eligible_countries as c
  on m.iso3 = c.iso3
