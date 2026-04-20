with raw_source as (
  select
    cast(country_code as integer) as country_code,
    {{ cast_string('iso3') }} as iso3_raw,
    {{ cast_string('country_name') }} as country_name_raw,
    {{ clean_label_text('region') }} as region,
    {{ clean_label_text('subregion') }} as subregion,
    {{ clean_label_text('continent') }} as continent,
    coalesce(cast(is_eu as boolean), false) as is_eu,
    coalesce(cast(is_oecd as boolean), false) as is_oecd
  from {{ source('raw', 'dim_country') }}
),
standardized as (
  select
    country_code,
    {{ canonical_country_iso3('iso3_raw') }} as iso3,
    country_name_raw,
    region,
    subregion,
    continent,
    is_eu,
    is_oecd
  from raw_source
),
canonicalized as (
  select
    country_code,
    iso3,
    {{ canonical_country_name('country_name_raw', 'iso3') }} as country_name,
    region,
    subregion,
    continent,
    is_eu,
    is_oecd
  from standardized
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
  case
    when country_code = 0 then true
    when iso3 in ('EUR', 'WLD', 'W00', 'A79', 'E19', 'F19', 'S19', 'X1', 'XX', '_X') then true
    when lower(country_name) in ('european union', 'world') then true
    when lower(coalesce(region, '')) in ('world', 'special') then true
    when lower(coalesce(continent, '')) in ('world', 'special') then true
    else false
  end as is_country_group,
  case
    when iso3 is null then false
    when not {{ regex_full_match('iso3', '^[A-Z]{3}$') }} then false
    when country_code = 0 then false
    when iso3 in ('EUR', 'WLD', 'W00', 'A79', 'E19', 'F19', 'S19', 'X1', 'XX', '_X') then false
    when lower(country_name) in ('european union', 'world') then false
    when lower(coalesce(region, '')) in ('world', 'special') then false
    when lower(coalesce(continent, '')) in ('world', 'special') then false
    else true
  end as is_country_map_eligible
from canonicalized
