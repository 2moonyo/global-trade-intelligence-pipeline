with raw_source as (
  select
    cast(ref_date as date) as ref_date,
    {{ safe_cast('period', dbt.type_int()) }} as period,
    case
      when {{ regex_full_match('year_month', '^\\d{4}-\\d{2}$') }} then {{ cast_string('year_month') }}
      when {{ safe_cast('period', dbt.type_int()) }} is not null then substr({{ cast_string('period') }}, 1, 4) || '-' || substr({{ cast_string('period') }}, 5, 2)
      else null
    end as year_month,
    coalesce(
      {{ safe_cast('ref_year', dbt.type_int()) }},
      {{ safe_cast('substr(' ~ cast_string('period') ~ ', 1, 4)', dbt.type_int()) }}
    ) as ref_year,
    upper(trim({{ cast_string('reporter_iso3') }})) as reporter_iso3,
    upper(trim({{ cast_string('partner_iso3') }})) as partner_iso3,
    trim({{ cast_string('cmdCode') }}) as cmd_code,
    case
      when upper(trim({{ cast_string('flowCode') }})) = 'M' then 'Import'
      when upper(trim({{ cast_string('flowCode') }})) = 'X' then 'Export'
      when flowCode is not null then trim({{ cast_string('flowCode') }})
      else null
    end as trade_flow,
    {{ cast_string('load_batch_id') }} as load_batch_id,
    {{ cast_string('source_file') }} as source_file,
    cast(bronze_extracted_at as timestamp) as bronze_extracted_at
  from {{ source('raw', 'comtrade_fact') }}
),

filtered as (
  select
    {{ hash_text(
      "coalesce(reporter_iso3, '') || '|' || "
      ~ "coalesce(partner_iso3, '') || '|' || "
      ~ "coalesce(cmd_code, '') || '|' || "
      ~ cast_string('period') ~ " || '|' || "
      ~ "coalesce(trade_flow, '')"
    ) }} as canonical_grain_key,
    reporter_iso3,
    partner_iso3,
    cmd_code,
    period,
    year_month,
    ref_year,
    trade_flow,
    load_batch_id,
    source_file,
    bronze_extracted_at
  from raw_source
  where period is not null
    and reporter_iso3 is not null
    and partner_iso3 is not null
    and cmd_code is not null
    and year_month is not null
    and trade_flow is not null
    and load_batch_id is not null
    and source_file is not null
),

aggregated as (
  select
    canonical_grain_key,
    reporter_iso3,
    partner_iso3,
    cmd_code,
    period,
    year_month,
    ref_year,
    trade_flow,
    load_batch_id,
    source_file,
    bronze_extracted_at,
    count(*) as raw_row_count
  from filtered
  group by 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11
)

select
  a.canonical_grain_key,
  a.reporter_iso3,
  a.partner_iso3,
  a.cmd_code,
  a.period,
  a.year_month,
  a.ref_year,
  a.trade_flow,
  a.load_batch_id,
  a.source_file,
  a.bronze_extracted_at,
  a.raw_row_count
from aggregated as a
inner join {{ ref('fct_reporter_partner_commodity_month') }} as f
  on a.canonical_grain_key = f.canonical_grain_key
