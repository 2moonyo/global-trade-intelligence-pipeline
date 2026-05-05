-- Grain: one row per reporter_country_code + month_start_date.
-- Purpose: dashboard-ready monthly trade overview for Page 1 scorecards,
-- trend lines, top reporter comparisons, and explicit completeness reporting
-- in Looker Studio.

with canonical_reporter_month as (
  select
    rmts.reporter_iso3 as reporter_country_code,
    rmts.reporter_country_name,
    rmts.reporter_region,
    rmts.reporter_is_eu,
    rmts.reporter_is_oecd,
    rmts.period as year_month_key,
    rmts.year_month,
    rmts.month_start_date,
    rmts.total_trade_value_usd,
    rmts.import_trade_value_usd,
    rmts.export_trade_value_usd,
    rmts.source_row_count
  from {{ ref('mart_reporter_month_trade_summary') }} as rmts
  where rmts.reporter_iso3 is not null
    and rmts.reporter_country_name is not null
    and rmts.month_start_date is not null
),

expected_reporters as (
  select
    crm.reporter_country_code,
    max(crm.reporter_country_name) as reporter_country_name,
    max(crm.reporter_region) as reporter_region,
    {{ bool_or('coalesce(crm.reporter_is_eu, false)') }} as reporter_is_eu,
    {{ bool_or('coalesce(crm.reporter_is_oecd, false)') }} as reporter_is_oecd
  from canonical_reporter_month as crm
  group by 1
),

month_spine as (
  select distinct
    crm.year_month_key,
    crm.year_month,
    crm.month_start_date
  from canonical_reporter_month as crm
),

reporter_month_grid as (
  select
    er.reporter_country_code,
    er.reporter_country_name,
    er.reporter_region,
    er.reporter_is_eu,
    er.reporter_is_oecd,
    ms.year_month_key,
    ms.year_month,
    ms.month_start_date
  from expected_reporters as er
  cross join month_spine as ms
),

grid_with_trade as (
  select
    g.reporter_country_code,
    g.reporter_country_name,
    g.reporter_region,
    g.reporter_is_eu,
    g.reporter_is_oecd,
    g.year_month_key,
    g.year_month,
    g.month_start_date,
    src.total_trade_value_usd,
    src.import_trade_value_usd,
    src.export_trade_value_usd,
    src.source_row_count,
    case
      when src.reporter_country_code is not null then true
      else false
    end as has_reported_trade_data_flag
  from reporter_month_grid as g
  left join canonical_reporter_month as src
    on g.reporter_country_code = src.reporter_country_code
   and g.year_month_key = src.year_month_key
),

month_completeness as (
  select
    gwt.year_month_key,
    gwt.year_month,
    gwt.month_start_date,
    count(*) as expected_reporter_count,
    countif(gwt.has_reported_trade_data_flag) as reporters_with_data_in_month,
    {{ safe_divide(
      'countif(gwt.has_reported_trade_data_flag)',
      'count(*)'
    ) }} as reporting_completeness_pct,
    case
      when countif(gwt.has_reported_trade_data_flag) = count(*) then true
      else false
    end as complete_month_flag
  from grid_with_trade as gwt
  group by 1, 2, 3
),

latest_month as (
  select
    max(ms.month_start_date) as latest_month_start_date
  from month_spine as ms
),

latest_complete_month as (
  select
    max(case when mc.complete_month_flag then mc.month_start_date end) as latest_complete_month_start_date
  from month_completeness as mc
),

coverage_enriched as (
  select
    gwt.reporter_country_code,
    gwt.reporter_country_name,
    gwt.reporter_region,
    gwt.reporter_is_eu,
    gwt.reporter_is_oecd,
    gwt.year_month_key,
    gwt.year_month,
    gwt.month_start_date,
    format_date('%b %Y', gwt.month_start_date) as month_label,
    gwt.has_reported_trade_data_flag,
    case
      when gwt.month_start_date = lm.latest_month_start_date
        and not gwt.has_reported_trade_data_flag then true
      else false
    end as missing_from_latest_month_flag,
    mc.reporters_with_data_in_month,
    mc.expected_reporter_count,
    mc.reporting_completeness_pct,
    mc.complete_month_flag,
    case
      when gwt.month_start_date = lm.latest_month_start_date then true
      else false
    end as latest_month_flag,
    case
      when lcm.latest_complete_month_start_date is not null
        and gwt.month_start_date = lcm.latest_complete_month_start_date then true
      else false
    end as latest_complete_month_flag,
    gwt.total_trade_value_usd,
    gwt.import_trade_value_usd,
    gwt.export_trade_value_usd,
    gwt.source_row_count
  from grid_with_trade as gwt
  inner join month_completeness as mc
    on gwt.year_month_key = mc.year_month_key
   and gwt.month_start_date = mc.month_start_date
  cross join latest_month as lm
  cross join latest_complete_month as lcm
),

comparison_ready as (
  select
    ce.reporter_country_code,
    ce.reporter_country_name,
    ce.reporter_region,
    ce.reporter_is_eu,
    ce.reporter_is_oecd,
    ce.year_month_key,
    ce.year_month,
    ce.month_start_date,
    ce.month_label,
    ce.has_reported_trade_data_flag,
    ce.missing_from_latest_month_flag,
    ce.reporters_with_data_in_month,
    ce.expected_reporter_count,
    ce.reporting_completeness_pct,
    ce.complete_month_flag,
    ce.latest_complete_month_flag,
    ce.latest_month_flag,
    ce.total_trade_value_usd,
    ce.import_trade_value_usd,
    ce.export_trade_value_usd,
    ce.source_row_count,
    lag(ce.month_start_date) over (
      partition by ce.reporter_country_code
      order by ce.month_start_date
    ) as previous_grid_month_start_date,
    lag(ce.total_trade_value_usd) over (
      partition by ce.reporter_country_code
      order by ce.month_start_date
    ) as previous_grid_total_trade_value_usd,
    lag(ce.import_trade_value_usd) over (
      partition by ce.reporter_country_code
      order by ce.month_start_date
    ) as previous_grid_import_trade_value_usd,
    lag(ce.export_trade_value_usd) over (
      partition by ce.reporter_country_code
      order by ce.month_start_date
    ) as previous_grid_export_trade_value_usd
  from coverage_enriched as ce
),

month_compared as (
  select
    cr.reporter_country_code,
    cr.reporter_country_name,
    cr.reporter_region,
    cr.reporter_is_eu,
    cr.reporter_is_oecd,
    cr.year_month_key,
    cr.year_month,
    cr.month_start_date,
    cr.month_label,
    cr.has_reported_trade_data_flag,
    cr.missing_from_latest_month_flag,
    cr.reporters_with_data_in_month,
    cr.expected_reporter_count,
    cr.reporting_completeness_pct,
    cr.complete_month_flag,
    cr.latest_complete_month_flag,
    cr.latest_month_flag,
    cr.total_trade_value_usd,
    cr.import_trade_value_usd,
    cr.export_trade_value_usd,
    cr.source_row_count,
    case
      when cr.previous_grid_month_start_date is not null
        and date_diff(cr.month_start_date, cr.previous_grid_month_start_date, month) = 1 then true
      else false
    end as previous_month_available_flag,
    case
      when cr.previous_grid_month_start_date is not null
        and date_diff(cr.month_start_date, cr.previous_grid_month_start_date, month) = 1
        then cr.previous_grid_total_trade_value_usd
      else null
    end as previous_month_total_trade_value_usd,
    case
      when cr.previous_grid_month_start_date is not null
        and date_diff(cr.month_start_date, cr.previous_grid_month_start_date, month) = 1
        then cr.previous_grid_import_trade_value_usd
      else null
    end as previous_month_import_trade_value_usd,
    case
      when cr.previous_grid_month_start_date is not null
        and date_diff(cr.month_start_date, cr.previous_grid_month_start_date, month) = 1
        then cr.previous_grid_export_trade_value_usd
      else null
    end as previous_month_export_trade_value_usd
  from comparison_ready as cr
),

ranked_reporters as (
  select
    mc.reporter_country_code,
    mc.month_start_date,
    row_number() over (
      partition by mc.month_start_date
      order by mc.total_trade_value_usd desc, mc.reporter_country_name
    ) as reporter_rank_by_total_trade_in_month
  from month_compared as mc
  where mc.has_reported_trade_data_flag
),

final as (
  select
    mc.reporter_country_code,
    mc.reporter_country_name,
    mc.reporter_region,
    mc.reporter_is_eu,
    mc.reporter_is_oecd,
    mc.year_month_key,
    mc.year_month,
    mc.month_start_date,
    mc.month_label,
    mc.has_reported_trade_data_flag,
    mc.missing_from_latest_month_flag,
    mc.reporters_with_data_in_month,
    mc.expected_reporter_count,
    mc.reporting_completeness_pct,
    mc.complete_month_flag,
    mc.latest_complete_month_flag,
    mc.latest_month_flag,
    mc.total_trade_value_usd,
    mc.import_trade_value_usd,
    mc.export_trade_value_usd,
    mc.source_row_count,
    mc.previous_month_available_flag,
    mc.previous_month_total_trade_value_usd,
    mc.previous_month_import_trade_value_usd,
    mc.previous_month_export_trade_value_usd,
    mc.total_trade_value_usd - mc.previous_month_total_trade_value_usd as total_trade_value_mom_change_usd,
    mc.import_trade_value_usd - mc.previous_month_import_trade_value_usd as import_trade_value_mom_change_usd,
    mc.export_trade_value_usd - mc.previous_month_export_trade_value_usd as export_trade_value_mom_change_usd,
    {{ safe_divide(
      'mc.total_trade_value_usd - mc.previous_month_total_trade_value_usd',
      'mc.previous_month_total_trade_value_usd'
    ) }} * 100 as total_trade_value_mom_change_pct,
    {{ safe_divide(
      'mc.import_trade_value_usd - mc.previous_month_import_trade_value_usd',
      'mc.previous_month_import_trade_value_usd'
    ) }} * 100 as import_trade_value_mom_change_pct,
    {{ safe_divide(
      'mc.export_trade_value_usd - mc.previous_month_export_trade_value_usd',
      'mc.previous_month_export_trade_value_usd'
    ) }} * 100 as export_trade_value_mom_change_pct,
    {{ format_compact_number('mc.total_trade_value_usd') }} as total_trade_value_label,
    {{ format_compact_number('mc.import_trade_value_usd') }} as import_trade_value_label,
    {{ format_compact_number('mc.export_trade_value_usd') }} as export_trade_value_label,
    case
      when {{ safe_divide(
        'mc.total_trade_value_usd - mc.previous_month_total_trade_value_usd',
        'mc.previous_month_total_trade_value_usd'
      ) }} * 100 is null then null
      else format(
        '%.2f%%',
        {{ safe_divide(
          'mc.total_trade_value_usd - mc.previous_month_total_trade_value_usd',
          'mc.previous_month_total_trade_value_usd'
        ) }} * 100
      )
    end as total_trade_value_mom_change_pct_label,
    case
      when {{ safe_divide(
        'mc.import_trade_value_usd - mc.previous_month_import_trade_value_usd',
        'mc.previous_month_import_trade_value_usd'
      ) }} * 100 is null then null
      else format(
        '%.2f%%',
        {{ safe_divide(
          'mc.import_trade_value_usd - mc.previous_month_import_trade_value_usd',
          'mc.previous_month_import_trade_value_usd'
        ) }} * 100
      )
    end as import_trade_value_mom_change_pct_label,
    case
      when {{ safe_divide(
        'mc.export_trade_value_usd - mc.previous_month_export_trade_value_usd',
        'mc.previous_month_export_trade_value_usd'
      ) }} * 100 is null then null
      else format(
        '%.2f%%',
        {{ safe_divide(
          'mc.export_trade_value_usd - mc.previous_month_export_trade_value_usd',
          'mc.previous_month_export_trade_value_usd'
        ) }} * 100
      )
    end as export_trade_value_mom_change_pct_label,
    rr.reporter_rank_by_total_trade_in_month,
    case
      when rr.reporter_rank_by_total_trade_in_month <= 5 then true
      else false
    end as top_5_reporter_in_month_flag
  from month_compared as mc
  left join ranked_reporters as rr
    on mc.reporter_country_code = rr.reporter_country_code
   and mc.month_start_date = rr.month_start_date
)

select
  reporter_country_code,
  reporter_country_name,
  reporter_region,
  reporter_is_eu,
  reporter_is_oecd,
  year_month_key,
  year_month,
  month_start_date,
  month_label,
  has_reported_trade_data_flag,
  missing_from_latest_month_flag,
  reporters_with_data_in_month,
  expected_reporter_count,
  reporting_completeness_pct,
  complete_month_flag,
  latest_complete_month_flag,
  latest_month_flag,
  total_trade_value_usd,
  import_trade_value_usd,
  export_trade_value_usd,
  source_row_count,
  previous_month_available_flag,
  previous_month_total_trade_value_usd,
  previous_month_import_trade_value_usd,
  previous_month_export_trade_value_usd,
  total_trade_value_mom_change_usd,
  import_trade_value_mom_change_usd,
  export_trade_value_mom_change_usd,
  total_trade_value_mom_change_pct,
  import_trade_value_mom_change_pct,
  export_trade_value_mom_change_pct,
  total_trade_value_label,
  import_trade_value_label,
  export_trade_value_label,
  total_trade_value_mom_change_pct_label,
  import_trade_value_mom_change_pct_label,
  export_trade_value_mom_change_pct_label,
  reporter_rank_by_total_trade_in_month,
  top_5_reporter_in_month_flag
from final
