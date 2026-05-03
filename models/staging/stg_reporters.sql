with raw_reporters as (
  select
    cast(reporterCode as {{ dbt.type_int() }}) as reporter_code,
    {{ clean_label_text('reporterDesc') }} as country_name_raw,
    {{ clean_label_text('reporterCodeIsoAlpha2') }} as country_iso2_raw,
    {{ canonical_country_iso3('reporterCodeIsoAlpha3') }} as country_iso3,
    coalesce(cast(isGroup as boolean), false) as is_group,
    case
      when {{ clean_label_text('entryExpiredDate') }} is null then null
      else cast(substr({{ cast_string('entryExpiredDate') }}, 1, 10) as date)
    end as entry_expired_date
  from {{ ref('reporters') }}
),
standardized as (
  select
    reporter_code,
    country_name_raw,
    {{ looker_country_name('country_name_raw', 'country_iso3') }} as country_name_looker,
    case
      when country_iso3 = 'NAM' and country_iso2_raw is null then 'NA'
      when country_iso2_raw is null then null
      else upper(trim(country_iso2_raw))
    end as country_iso2,
    country_iso3,
    is_group,
    entry_expired_date
  from raw_reporters
)

select
  reporter_code,
  country_name_raw,
  country_name_looker,
  country_iso2,
  country_iso3,
  is_group,
  case
    when entry_expired_date is null then true
    when entry_expired_date >= {% if target.type == 'bigquery' %}current_date(){% else %}current_date{% endif %} then true
    else false
  end as is_current,
  case
    when is_group then false
    when country_iso3 is null then false
    when not {{ regex_full_match('country_iso3', '^[A-Z]{3}$') }} then false
    when country_iso2 is null then false
    when not {{ regex_full_match('country_iso2', '^[A-Z]{2}$') }} then false
    when not (
      case
        when entry_expired_date is null then true
        when entry_expired_date >= {% if target.type == 'bigquery' %}current_date(){% else %}current_date{% endif %} then true
        else false
      end
    ) then false
    when country_iso3 in ('EUR', 'WLD', 'W00', 'A79', 'E19', 'F19', 'S19', 'X1', 'XX', '_X') then false
    when lower(country_name_raw) in ('european union', 'asean', 'other asia, nes', 'world') then false
    else true
  end as is_map_eligible
from standardized
