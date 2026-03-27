with raw_source as (
  select
    cast(ref_date as date) as ref_date,
    try_cast(period as integer) as period,
    case
      when regexp_full_match(cast(year_month as varchar), '^\\d{4}-\\d{2}$') then cast(year_month as varchar)
      when try_cast(period as integer) is not null then substr(cast(period as varchar), 1, 4) || '-' || substr(cast(period as varchar), 5, 2)
      else null
    end as year_month,
    coalesce(
      try_cast(ref_year as integer),
      try_cast(substr(cast(period as varchar), 1, 4) as integer)
    ) as ref_year,
    upper(trim(cast(reporter_iso3 as varchar))) as reporter_iso3,
    upper(trim(cast(partner_iso3 as varchar))) as partner_iso3,
    trim(cast(cmdCode as varchar)) as cmd_code,
    case
      when upper(trim(cast(flowCode as varchar))) = 'M' then 'Import'
      when upper(trim(cast(flowCode as varchar))) = 'X' then 'Export'
      when flowCode is not null then trim(cast(flowCode as varchar))
      else null
    end as trade_flow,
    cast(load_batch_id as varchar) as load_batch_id,
    cast(source_file as varchar) as source_file,
    cast(bronze_extracted_at as timestamp with time zone) as bronze_extracted_at
  from "analytics"."raw"."comtrade_fact"
),

filtered as (
  select
    md5(
      coalesce(reporter_iso3, '') || '|' ||
      coalesce(partner_iso3, '') || '|' ||
      coalesce(cmd_code, '') || '|' ||
      cast(period as varchar) || '|' ||
      coalesce(trade_flow, '')
    ) as canonical_grain_key,
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
    count(*) as raw_row_count,
    count(distinct load_batch_id) as distinct_load_batch_count,
    count(distinct source_file) as distinct_source_file_count,
    min(bronze_extracted_at) as first_bronze_extracted_at,
    max(bronze_extracted_at) as last_bronze_extracted_at,
    list(distinct load_batch_id) as load_batch_ids,
    list(distinct source_file) as source_files
  from filtered
  group by 1, 2, 3, 4, 5, 6, 7, 8
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
  a.raw_row_count,
  a.distinct_load_batch_count,
  a.distinct_source_file_count,
  a.first_bronze_extracted_at,
  a.last_bronze_extracted_at,
  a.load_batch_ids,
  a.source_files
from aggregated as a
inner join "analytics"."analytics_marts"."fct_reporter_partner_commodity_month" as f
  on a.canonical_grain_key = f.canonical_grain_key