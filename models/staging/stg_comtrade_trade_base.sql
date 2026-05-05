with raw_source as (
  select
    cast(ref_date as date) as ref_date,
    {{ cast_string('period') }} as period_raw,
    {{ cast_string('ref_year') }} as ref_year_raw,
    {{ cast_string('year_month') }} as year_month_raw,
    {{ cast_string('reporter_iso3') }} as reporter_iso3_raw,
    {{ cast_string('partner_iso3') }} as partner_iso3_raw,
    {{ cast_string('cmdCode') }} as cmd_code_raw,
    {{ cast_string('cmdDesc') }} as commodity_name_raw,
    {{ cast_string('flowCode') }} as flow_code_raw,
    {{ cast_float('trade_value_usd') }} as trade_value_usd,
    {{ cast_float('netWgt') }} as net_weight_kg,
    {{ cast_float('grossWgt') }} as gross_weight_kg,
    {{ cast_float('qty') }} as qty,
    cast(motCode as integer) as mot_code,
    cast(partner2Code as integer) as partner2_code
  from {{ source('raw', 'comtrade_fact') }}
),
source_data as (
  select
    ref_date,
    {{ safe_cast('period_raw', dbt.type_int()) }} as period,
    case
      when {{ regex_full_match('year_month_raw', '^\\d{4}-\\d{2}$') }} then year_month_raw
      when {{ safe_cast('period_raw', dbt.type_int()) }} is not null then substr(period_raw, 1, 4) || '-' || substr(period_raw, 5, 2)
      else null
    end as year_month,
    coalesce(
      {{ safe_cast('ref_year_raw', dbt.type_int()) }},
      {{ safe_cast('substr(period_raw, 1, 4)', dbt.type_int()) }}
    ) as ref_year,
    {{ canonical_country_iso3('reporter_iso3_raw') }} as reporter_iso3,
    {{ canonical_country_iso3('partner_iso3_raw') }} as partner_iso3,
    trim(cmd_code_raw) as cmd_code,
    trim(commodity_name_raw) as commodity_name_raw,
    case
      when upper(trim(flow_code_raw)) = 'M' then 'Import'
      when upper(trim(flow_code_raw)) = 'X' then 'Export'
      when flow_code_raw is not null then trim(flow_code_raw)
      else null
    end as trade_flow,
    trade_value_usd,
    net_weight_kg,
    gross_weight_kg,
    qty,
    mot_code,
    partner2_code
  from raw_source
)

select
  distinct
  {{ hash_text(
    "coalesce(reporter_iso3, '') || '|' || "
    ~ "coalesce(partner_iso3, '') || '|' || "
    ~ "coalesce(cmd_code, '') || '|' || "
    ~ cast_string('period') ~ " || '|' || "
    ~ "coalesce(trade_flow, '')"
  ) }} as canonical_grain_key,
  ref_date,
  period,
  year_month,
  ref_year,
  reporter_iso3,
  partner_iso3,
  cmd_code,
  commodity_name_raw,
  trade_flow,
  trade_value_usd,
  net_weight_kg,
  gross_weight_kg,
  qty,
  mot_code,
  partner2_code
from source_data
where period is not null
  and reporter_iso3 is not null
  and partner_iso3 is not null
  and cmd_code is not null
  and year_month is not null
  and trade_flow is not null
