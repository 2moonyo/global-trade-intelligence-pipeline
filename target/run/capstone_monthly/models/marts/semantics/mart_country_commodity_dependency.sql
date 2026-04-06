
  
    

    create or replace table `capfractal`.`analytics_marts`.`mart_country_commodity_dependency`
      
    
    

    
    OPTIONS()
    as (
      -- Grain: one row per reporter_country_code + commodity_code + period.
-- Purpose: business-facing commodity dependency view with shares, ranks, and concentration cues.

with commodity_trade_month as (
  -- Reuse existing reporter-commodity monthly mart to preserve canonical aggregation logic.
  select
    rc.reporter_iso3 as reporter_country_code,
    rc.reporter_country_name,
    rc.cmd_code as commodity_code,
    rc.commodity_name,
    rc.commodity_group,
    rc.food_flag,
    rc.energy_flag,
    rc.industrial_flag,
    rc.period,
    rc.year_month,
    rc.month_start_date,
    rc.year,
    rc.month,
    rc.total_trade_value_usd as commodity_trade_value_usd,
    rc.import_trade_value_usd as commodity_import_value_usd,
    rc.export_trade_value_usd as commodity_export_value_usd,
    rc.total_net_weight_kg as commodity_net_weight_kg,
    rc.total_gross_weight_kg as commodity_gross_weight_kg
  from `capfractal`.`analytics_marts`.`mart_reporter_commodity_month_trade_summary` as rc
),
reporter_totals_month as (
  -- Reporter totals provide denominator for commodity share percentages.
  select
    reporter_iso3 as reporter_country_code,
    period,
    year_month,
    total_trade_value_usd as reporter_trade_value_usd
  from `capfractal`.`analytics_marts`.`mart_reporter_month_trade_summary`
),
commodity_share_metrics as (
  -- Join aligns commodity numerators with reporter-month denominator.
  select
    ctm.reporter_country_code,
    ctm.reporter_country_name,
    ctm.commodity_code,
    ctm.commodity_name,
    ctm.commodity_group,
    ctm.food_flag,
    ctm.energy_flag,
    ctm.industrial_flag,
    ctm.period,
    ctm.year_month,
    ctm.month_start_date,
    ctm.year,
    ctm.month,
    ctm.commodity_trade_value_usd,
    ctm.commodity_import_value_usd,
    ctm.commodity_export_value_usd,
    ctm.commodity_net_weight_kg,
    ctm.commodity_gross_weight_kg,
    rtm.reporter_trade_value_usd,
    greatest(ctm.commodity_trade_value_usd, 0) as commodity_trade_value_for_share_usd,
    sum(greatest(ctm.commodity_trade_value_usd, 0)) over (
      partition by ctm.reporter_country_code, ctm.period, ctm.year_month
    ) as reporter_trade_value_for_share_usd,
    case
    when sum(greatest(ctm.commodity_trade_value_usd, 0)) over (partition by ctm.reporter_country_code, ctm.period, ctm.year_month) is null or sum(greatest(ctm.commodity_trade_value_usd, 0)) over (partition by ctm.reporter_country_code, ctm.period, ctm.year_month) = 0 then null
    else greatest(ctm.commodity_trade_value_usd, 0) / sum(greatest(ctm.commodity_trade_value_usd, 0)) over (partition by ctm.reporter_country_code, ctm.period, ctm.year_month)
  end * 100 as commodity_trade_share_pct
  from commodity_trade_month as ctm
  left join reporter_totals_month as rtm
    on ctm.reporter_country_code = rtm.reporter_country_code
   and ctm.period = rtm.period
   and ctm.year_month = rtm.year_month
),
ranked_commodities as (
  -- Ranking supports top-commodity bar charts and contribution tables.
  select
    csm.reporter_country_code,
    csm.reporter_country_name,
    csm.commodity_code,
    csm.commodity_name,
    csm.commodity_group,
    csm.food_flag,
    csm.energy_flag,
    csm.industrial_flag,
    csm.period,
    csm.year_month,
    csm.month_start_date,
    csm.year,
    csm.month,
    csm.commodity_trade_value_usd,
    csm.commodity_import_value_usd,
    csm.commodity_export_value_usd,
    csm.commodity_net_weight_kg,
    csm.commodity_gross_weight_kg,
    csm.reporter_trade_value_usd,
    csm.commodity_trade_value_for_share_usd,
    csm.reporter_trade_value_for_share_usd,
    csm.commodity_trade_share_pct,
    dense_rank() over (
      partition by csm.reporter_country_code, csm.year_month
      order by csm.commodity_trade_value_usd desc, csm.commodity_code
    ) as commodity_rank_by_trade_value,
    sum(csm.commodity_trade_value_for_share_usd) over (
      partition by csm.reporter_country_code, csm.year_month
      order by csm.commodity_trade_value_usd desc, csm.commodity_code
      rows between unbounded preceding and current row
    ) as cumulative_commodity_trade_value_for_share_usd
  from commodity_share_metrics as csm
),
scored_commodities as (
  -- Dependency and risk categories provide simple qualitative interpretation of commodity concentration.
  select
    rc.reporter_country_code,
    rc.reporter_country_name,
    rc.commodity_code,
    rc.commodity_name,
    rc.commodity_group,
    rc.food_flag,
    rc.energy_flag,
    rc.industrial_flag,
    rc.period,
    rc.year_month,
    rc.month_start_date,
    rc.year,
    rc.month,
    rc.commodity_trade_value_usd,
    rc.commodity_import_value_usd,
    rc.commodity_export_value_usd,
    rc.commodity_net_weight_kg,
    rc.commodity_gross_weight_kg,
    rc.reporter_trade_value_usd,
    rc.commodity_trade_share_pct,
    case
    when rc.reporter_trade_value_for_share_usd is null or rc.reporter_trade_value_for_share_usd = 0 then null
    else rc.cumulative_commodity_trade_value_for_share_usd / rc.reporter_trade_value_for_share_usd
  end * 100 as cumulative_commodity_share_pct,
    rc.commodity_rank_by_trade_value,
    case
      when rc.commodity_trade_share_pct >= 25 then 'very_high'
      when rc.commodity_trade_share_pct >= 15 then 'high'
      when rc.commodity_trade_share_pct >= 8 then 'moderate'
      when rc.commodity_trade_share_pct >= 3 then 'low'
      else 'very_low'
    end as dependency_level,
    case
      when rc.commodity_trade_share_pct >= 20 and rc.commodity_rank_by_trade_value <= 3 then 'high'
      when rc.commodity_trade_share_pct >= 10 then 'medium'
      else 'low'
    end as risk_level
  from ranked_commodities as rc
),
final as (
  -- Final semantic output keeps numeric values and human-readable labels in one mart.
  select
    sc.reporter_country_code,
    sc.reporter_country_name,
    sc.commodity_code,
    sc.commodity_name,
    sc.commodity_group,
    sc.food_flag,
    sc.energy_flag,
    sc.industrial_flag,
    sc.period as year_month_key,
    sc.year_month,
    sc.month_start_date,
    sc.year,
    format_date('%b', sc.month_start_date) as month_name_short,
    format_date('%b %Y', sc.month_start_date) as month_label,
    sc.commodity_trade_value_usd,
    sc.commodity_import_value_usd,
    sc.commodity_export_value_usd,
    sc.commodity_net_weight_kg,
    sc.commodity_gross_weight_kg,
    sc.reporter_trade_value_usd,
    sc.commodity_trade_value_usd / 1000000000.0 as commodity_trade_value_billion,
    case
      when sc.commodity_trade_value_usd is null then null
      when abs(sc.commodity_trade_value_usd) >= 1000000000000 then concat(format('%.2f', sc.commodity_trade_value_usd / 1000000000000.0), ' trillion')
      when abs(sc.commodity_trade_value_usd) >= 1000000000 then concat(format('%.2f', sc.commodity_trade_value_usd / 1000000000.0), ' billion')
      when abs(sc.commodity_trade_value_usd) >= 1000000 then concat(format('%.2f', sc.commodity_trade_value_usd / 1000000.0), ' million')
      when abs(sc.commodity_trade_value_usd) >= 1000 then concat(format('%.1f', sc.commodity_trade_value_usd / 1000.0), ' thousand')
      else format('%.0f', sc.commodity_trade_value_usd)
    end as commodity_trade_value_label,
    sc.commodity_trade_share_pct,
    format('%.1f%%', sc.commodity_trade_share_pct) as commodity_trade_share_label,
    sc.cumulative_commodity_share_pct,
    format('%.1f%%', sc.cumulative_commodity_share_pct) as cumulative_commodity_share_label,
    sc.commodity_rank_by_trade_value,
    case when sc.commodity_rank_by_trade_value <= 10 then true else false end as is_top_10_commodity,
    sc.dependency_level,
    sc.risk_level
  from scored_commodities as sc
)

select
  reporter_country_code,
  reporter_country_name,
  commodity_code,
  commodity_name,
  commodity_group,
  food_flag,
  energy_flag,
  industrial_flag,
  year_month_key,
  year_month,
  month_start_date,
  year,
  month_name_short,
  month_label,
  commodity_trade_value_usd,
  commodity_import_value_usd,
  commodity_export_value_usd,
  commodity_net_weight_kg,
  commodity_gross_weight_kg,
  reporter_trade_value_usd,
  commodity_trade_value_billion,
  commodity_trade_value_label,
  commodity_trade_share_pct,
  commodity_trade_share_label,
  cumulative_commodity_share_pct,
  cumulative_commodity_share_label,
  commodity_rank_by_trade_value,
  is_top_10_commodity,
  dependency_level,
  risk_level
from final
    );
  