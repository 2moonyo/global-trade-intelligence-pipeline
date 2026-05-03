with reporter_metadata as (
  select
    reporter_code,
    country_name_raw,
    country_name_looker,
    country_iso2,
    country_iso3,
    is_group,
    is_current,
    is_map_eligible
  from {{ ref('stg_reporters') }}
  where country_iso3 is not null
),
reporter_metadata_ranked as (
  select
    *,
    row_number() over (
      partition by country_iso3
      order by
        case when is_current then 0 else 1 end,
        case when is_map_eligible then 0 else 1 end,
        case when is_group then 1 else 0 end,
        reporter_code desc
    ) as reporter_rank
  from reporter_metadata
),
country_attributes as (
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
    is_country_map_eligible,
    row_number() over (
      partition by iso3
      order by
        case when is_country_map_eligible then 0 else 1 end,
        case when country_code is null then 1 else 0 end,
        country_code
    ) as country_rank
  from {{ ref('stg_dim_country') }}
  where iso3 is not null
),
port_coordinates as (
  select
    iso3,
    avg(latitude) as latitude,
    avg(longitude) as longitude
  from {{ ref('stg_dim_country_ports') }}
  where latitude between -90 and 90
    and longitude between -180 and 180
  group by 1
),
enriched as (
  select
    rm.reporter_code as country_code,
    rm.reporter_code,
    rm.country_iso3 as iso3,
    coalesce(ca.country_name, {{ canonical_country_name('rm.country_name_raw', 'rm.country_iso3') }}) as country_name,
    rm.country_name_raw,
    rm.country_name_looker,
    rm.country_iso2,
    rm.country_iso3,
    ca.region,
    ca.subregion,
    ca.continent,
    coalesce(ca.is_eu, false) as is_eu,
    coalesce(ca.is_oecd, false) as is_oecd,
    coalesce(ca.is_country_group, rm.is_group) as is_country_group,
    rm.is_current,
    rm.is_map_eligible as is_country_map_eligible,
    pc.latitude,
    pc.longitude,
    case
      when pc.longitude is not null and pc.latitude is not null
        then {{ geography_point('pc.longitude', 'pc.latitude') }}
      else null
    end as geo_point,
    case
      when pc.latitude is not null and pc.longitude is not null
        then concat(
          cast(pc.latitude as {{ dbt.type_string() }}),
          ',',
          cast(pc.longitude as {{ dbt.type_string() }})
        )
      else null
    end as lat_lng_string
  from reporter_metadata_ranked as rm
  left join country_attributes as ca
    on rm.country_iso3 = ca.iso3
   and ca.country_rank = 1
  left join port_coordinates as pc
    on rm.country_iso3 = pc.iso3
  where rm.reporter_rank = 1
),
fallback_raw_countries as (
  select
    ca.country_code,
    ca.country_code as reporter_code,
    ca.iso3,
    ca.country_name,
    ca.country_name as country_name_raw,
    {{ looker_country_name('ca.country_name', 'ca.iso3') }} as country_name_looker,
    cast(null as {{ dbt.type_string() }}) as country_iso2,
    ca.iso3 as country_iso3,
    ca.region,
    ca.subregion,
    ca.continent,
    coalesce(ca.is_eu, false) as is_eu,
    coalesce(ca.is_oecd, false) as is_oecd,
    coalesce(ca.is_country_group, false) as is_country_group,
    true as is_current,
    false as is_country_map_eligible,
    pc.latitude,
    pc.longitude,
    case
      when pc.longitude is not null and pc.latitude is not null
        then {{ geography_point('pc.longitude', 'pc.latitude') }}
      else null
    end as geo_point,
    case
      when pc.latitude is not null and pc.longitude is not null
        then concat(
          cast(pc.latitude as {{ dbt.type_string() }}),
          ',',
          cast(pc.longitude as {{ dbt.type_string() }})
        )
      else null
    end as lat_lng_string
  from country_attributes as ca
  left join reporter_metadata_ranked as rm
    on ca.iso3 = rm.country_iso3
   and rm.reporter_rank = 1
  left join port_coordinates as pc
    on ca.iso3 = pc.iso3
  where ca.country_rank = 1
    and rm.country_iso3 is null
)

select
  country_code,
  reporter_code,
  iso3,
  country_name,
  country_name_raw,
  country_name_looker,
  country_iso2,
  country_iso3,
  region,
  subregion,
  continent,
  is_eu,
  is_oecd,
  is_country_group,
  is_current,
  is_country_map_eligible,
  latitude,
  longitude,
  geo_point,
  lat_lng_string
from enriched
union all
select
  country_code,
  reporter_code,
  iso3,
  country_name,
  country_name_raw,
  country_name_looker,
  country_iso2,
  country_iso3,
  region,
  subregion,
  continent,
  is_eu,
  is_oecd,
  is_country_group,
  is_current,
  is_country_map_eligible,
  latitude,
  longitude,
  geo_point,
  lat_lng_string
from fallback_raw_countries
