-- Grain: one row per reporter_country_code + partner_country_code + period.
-- Purpose: business-facing dependency view for partner concentration, rankings, and monthly shares.

with partner_trade_month as (
  -- Aggregate canonical fact to reporter-partner-month for dashboard filters and comparisons.
  select
    f.reporter_iso3 as reporter_country_code,
    f.partner_iso3 as partner_country_code,
    f.period,
    f.year_month,
    sum(f.trade_value_usd) as partner_trade_value_usd,
    sum(case when lower(f.trade_flow) like '%import%' then f.trade_value_usd else 0 end) as partner_import_value_usd,
    sum(case when lower(f.trade_flow) like '%export%' then f.trade_value_usd else 0 end) as partner_export_value_usd,
    sum(f.net_weight_kg) as partner_net_weight_kg,
    count(distinct f.cmd_code) as commodity_count
  from {{ ref('fct_reporter_partner_commodity_month') }} as f
  group by 1, 2, 3, 4
),
reporter_totals_month as (
  -- Use the existing reporter-month summary as denominator for stable share calculations.
  select
    reporter_iso3 as reporter_country_code,
    period,
    year_month,
    total_trade_value_usd as reporter_trade_value_usd
  from {{ ref('mart_reporter_month_trade_summary') }}
),
partner_share_metrics as (
  -- Join aligns partner numerators with the reporter-month denominator at the same grain.
  select
    ptm.reporter_country_code,
    ptm.partner_country_code,
    ptm.period,
    ptm.year_month,
    ptm.partner_trade_value_usd,
    ptm.partner_import_value_usd,
    ptm.partner_export_value_usd,
    ptm.partner_net_weight_kg,
    ptm.commodity_count,
    rtm.reporter_trade_value_usd,
    greatest(ptm.partner_trade_value_usd, 0) as partner_trade_value_for_share_usd,
    sum(greatest(ptm.partner_trade_value_usd, 0)) over (
      partition by ptm.reporter_country_code, ptm.period, ptm.year_month
    ) as reporter_trade_value_for_share_usd,
    {{ safe_divide(
      'greatest(ptm.partner_trade_value_usd, 0)',
      'sum(greatest(ptm.partner_trade_value_usd, 0)) over (partition by ptm.reporter_country_code, ptm.period, ptm.year_month)'
    ) }} * 100 as partner_trade_share_pct
  from partner_trade_month as ptm
  left join reporter_totals_month as rtm
    on ptm.reporter_country_code = rtm.reporter_country_code
   and ptm.period = rtm.period
   and ptm.year_month = rtm.year_month
),
ranked_partners as (
  -- Ranking and cumulative values support top-partner charts and concentration storytelling.
  select
    psm.reporter_country_code,
    psm.partner_country_code,
    psm.period,
    psm.year_month,
    psm.partner_trade_value_usd,
    psm.partner_import_value_usd,
    psm.partner_export_value_usd,
    psm.partner_net_weight_kg,
    psm.commodity_count,
    psm.reporter_trade_value_usd,
    psm.partner_trade_value_for_share_usd,
    psm.reporter_trade_value_for_share_usd,
    psm.partner_trade_share_pct,
    dense_rank() over (
      partition by psm.reporter_country_code, psm.year_month
      order by psm.partner_trade_value_usd desc, psm.partner_country_code
    ) as partner_rank_by_trade_value,
    sum(psm.partner_trade_value_for_share_usd) over (
      partition by psm.reporter_country_code, psm.year_month
      order by psm.partner_trade_value_usd desc, psm.partner_country_code
      rows between unbounded preceding and current row
    ) as cumulative_partner_trade_value_for_share_usd
  from partner_share_metrics as psm
),
dependency_metrics as (
  -- Dependency and risk tiers are business-facing categories based on partner share magnitude.
  select
    rp.reporter_country_code,
    rp.partner_country_code,
    rp.period,
    rp.year_month,
    rp.partner_trade_value_usd,
    rp.partner_import_value_usd,
    rp.partner_export_value_usd,
    rp.partner_net_weight_kg,
    rp.commodity_count,
    rp.reporter_trade_value_usd,
    rp.partner_trade_share_pct,
    {{ safe_divide('rp.cumulative_partner_trade_value_for_share_usd', 'rp.reporter_trade_value_for_share_usd') }} * 100 as cumulative_partner_share_pct,
    rp.partner_rank_by_trade_value,
    case
      when rp.partner_trade_share_pct >= 30 then 'very_high'
      when rp.partner_trade_share_pct >= 20 then 'high'
      when rp.partner_trade_share_pct >= 10 then 'moderate'
      when rp.partner_trade_share_pct >= 5 then 'low'
      else 'very_low'
    end as dependency_level,
    case
      when rp.partner_trade_share_pct >= 25 and rp.partner_rank_by_trade_value <= 3 then 'high'
      when rp.partner_trade_share_pct >= 12 then 'medium'
      else 'low'
    end as risk_level
  from ranked_partners as rp
),
dim_enriched as (
  -- Join adds readable country and calendar fields so Looker can use direct filters without extra joins.
  select
    dm.reporter_country_code,
    reporter_dim.country_name as reporter_country_name,
    reporter_dim.region as reporter_region,
    reporter_dim.subregion as reporter_subregion,
    reporter_dim.continent as reporter_continent,
    dm.partner_country_code,
    partner_dim.country_name as partner_country_name,
    dm.period,
    dm.year_month,
    t.month_start_date,
    t.year,
    t.month,
    dm.partner_trade_value_usd,
    dm.partner_import_value_usd,
    dm.partner_export_value_usd,
    dm.partner_net_weight_kg,
    dm.commodity_count,
    dm.reporter_trade_value_usd,
    dm.partner_trade_share_pct,
    dm.cumulative_partner_share_pct,
    dm.partner_rank_by_trade_value,
    dm.dependency_level,
    dm.risk_level
  from dependency_metrics as dm
  -- Join maps reporter ISO3 to readable name and region attributes.
  left join {{ ref('dim_country') }} as reporter_dim
    on dm.reporter_country_code = reporter_dim.iso3
  -- Join maps partner ISO3 to readable name for partner comparison charts.
  left join {{ ref('dim_country') }} as partner_dim
    on dm.partner_country_code = partner_dim.iso3
  -- Join standardizes month fields to the conformed calendar dimension.
  left join {{ ref('dim_time') }} as t
    on dm.period = t.period
),
final as (
  -- Final semantic output keeps machine-safe numerics and display labels side-by-side.
  select
    {{ hash_text("concat(coalesce(de.reporter_country_code, ''), '|', coalesce(de.partner_country_code, ''))") }} as reporter_partner_key,
    de.reporter_country_code,
    de.reporter_country_name,
    de.reporter_region,
    de.reporter_subregion,
    de.reporter_continent,
    de.partner_country_code,
    de.partner_country_name,
    de.period as year_month_key,
    de.year_month,
    de.month_start_date,
    de.year,
    format_date('%b', de.month_start_date) as month_name_short,
    format_date('%b %Y', de.month_start_date) as month_label,
    de.partner_trade_value_usd,
    de.partner_import_value_usd,
    de.partner_export_value_usd,
    de.partner_net_weight_kg,
    de.commodity_count,
    de.reporter_trade_value_usd,
    de.partner_trade_value_usd / 1000000000.0 as partner_trade_value_billion,
    case
      when de.partner_trade_value_usd is null then null
      when abs(de.partner_trade_value_usd) >= 1000000000000 then concat(format('%.2f', de.partner_trade_value_usd / 1000000000000.0), ' trillion')
      when abs(de.partner_trade_value_usd) >= 1000000000 then concat(format('%.2f', de.partner_trade_value_usd / 1000000000.0), ' billion')
      when abs(de.partner_trade_value_usd) >= 1000000 then concat(format('%.2f', de.partner_trade_value_usd / 1000000.0), ' million')
      when abs(de.partner_trade_value_usd) >= 1000 then concat(format('%.1f', de.partner_trade_value_usd / 1000.0), ' thousand')
      else format('%.0f', de.partner_trade_value_usd)
    end as partner_trade_value_label,
    de.partner_trade_share_pct,
    format('%.1f%%', de.partner_trade_share_pct) as partner_trade_share_label,
    de.cumulative_partner_share_pct,
    format('%.1f%%', de.cumulative_partner_share_pct) as cumulative_partner_share_label,
    de.partner_rank_by_trade_value,
    case when de.partner_rank_by_trade_value <= 10 then true else false end as is_top_10_partner,
    de.dependency_level,
    de.risk_level
  from dim_enriched as de
)

select
  reporter_partner_key,
  reporter_country_code,
  reporter_country_name,
  reporter_region,
  reporter_subregion,
  reporter_continent,
  partner_country_code,
  partner_country_name,
  year_month_key,
  year_month,
  month_start_date,
  year,
  month_name_short,
  month_label,
  partner_trade_value_usd,
  partner_import_value_usd,
  partner_export_value_usd,
  partner_net_weight_kg,
  commodity_count,
  reporter_trade_value_usd,
  partner_trade_value_billion,
  partner_trade_value_label,
  partner_trade_share_pct,
  partner_trade_share_label,
  cumulative_partner_share_pct,
  cumulative_partner_share_label,
  partner_rank_by_trade_value,
  is_top_10_partner,
  dependency_level,
  risk_level
from final
