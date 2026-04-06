-- Grain: one row per reporter_country_code + period.
-- Purpose: headline monthly trade profile with simple trends, shares, and directional signals.

with reporter_month_base as (
  -- Base metrics come from the existing reporter-month summary to preserve canonical totals.
  select
    rmts.reporter_iso3 as reporter_country_code,
    rmts.reporter_country_name,
    rmts.reporter_region,
    rmts.reporter_subregion,
    rmts.reporter_continent,
    rmts.period,
    rmts.year_month,
    rmts.month_start_date,
    rmts.year,
    rmts.month,
    rmts.total_trade_value_usd,
    rmts.import_trade_value_usd,
    rmts.export_trade_value_usd,
    rmts.total_net_weight_kg,
    rmts.total_gross_weight_kg
  from `capfractal`.`analytics_marts`.`mart_reporter_month_trade_summary` as rmts
),
growth_metrics as (
  -- Prior month values support month-over-month movement metrics.
  select
    rmb.reporter_country_code,
    rmb.reporter_country_name,
    rmb.reporter_region,
    rmb.reporter_subregion,
    rmb.reporter_continent,
    rmb.period,
    rmb.year_month,
    rmb.month_start_date,
    rmb.year,
    rmb.month,
    rmb.total_trade_value_usd,
    rmb.import_trade_value_usd,
    rmb.export_trade_value_usd,
    rmb.total_net_weight_kg,
    rmb.total_gross_weight_kg,
    lag(rmb.total_trade_value_usd) over (
      partition by rmb.reporter_country_code
      order by rmb.period
    ) as previous_month_trade_value_usd
  from reporter_month_base as rmb
),
ranked_reporters as (
  -- Reporter ranking enables monthly comparison scorecards and league tables.
  select
    gm.reporter_country_code,
    gm.reporter_country_name,
    gm.reporter_region,
    gm.reporter_subregion,
    gm.reporter_continent,
    gm.period,
    gm.year_month,
    gm.month_start_date,
    gm.year,
    gm.month,
    gm.total_trade_value_usd,
    gm.import_trade_value_usd,
    gm.export_trade_value_usd,
    gm.total_net_weight_kg,
    gm.total_gross_weight_kg,
    gm.previous_month_trade_value_usd,
    dense_rank() over (
      partition by gm.year_month
      order by gm.total_trade_value_usd desc, gm.reporter_country_code
    ) as reporter_rank_by_trade_value
  from growth_metrics as gm
),
scored_profile as (
  -- Derived percentages and orientation labels are designed for plain-language chart narratives.
  select
    rr.reporter_country_code,
    rr.reporter_country_name,
    rr.reporter_region,
    rr.reporter_subregion,
    rr.reporter_continent,
    rr.period,
    rr.year_month,
    rr.month_start_date,
    rr.year,
    rr.month,
    rr.total_trade_value_usd,
    rr.import_trade_value_usd,
    rr.export_trade_value_usd,
    rr.total_net_weight_kg,
    rr.total_gross_weight_kg,
    rr.previous_month_trade_value_usd,
    rr.reporter_rank_by_trade_value,
    rr.export_trade_value_usd - rr.import_trade_value_usd as trade_balance_usd,
    case
    when rr.total_trade_value_usd is null or rr.total_trade_value_usd = 0 then null
    else rr.import_trade_value_usd / rr.total_trade_value_usd
  end * 100 as import_trade_share_pct,
    case
    when rr.total_trade_value_usd is null or rr.total_trade_value_usd = 0 then null
    else rr.export_trade_value_usd / rr.total_trade_value_usd
  end * 100 as export_trade_share_pct,
    case
    when rr.previous_month_trade_value_usd is null or rr.previous_month_trade_value_usd = 0 then null
    else rr.total_trade_value_usd - rr.previous_month_trade_value_usd / rr.previous_month_trade_value_usd
  end * 100 as trade_value_mom_change_pct,
    case
      when rr.export_trade_value_usd - rr.import_trade_value_usd > 0 then 'surplus'
      when rr.export_trade_value_usd - rr.import_trade_value_usd < 0 then 'deficit'
      else 'balanced'
    end as trade_balance_direction,
    case
      when case
    when rr.total_trade_value_usd is null or rr.total_trade_value_usd = 0 then null
    else rr.export_trade_value_usd / rr.total_trade_value_usd
  end * 100 >= 55 then 'export_oriented'
      when case
    when rr.total_trade_value_usd is null or rr.total_trade_value_usd = 0 then null
    else rr.import_trade_value_usd / rr.total_trade_value_usd
  end * 100 >= 55 then 'import_oriented'
      else 'mixed'
    end as trade_orientation
  from ranked_reporters as rr
),
final as (
  -- Final projection keeps sortable numerics and readable labels for dashboard cards and tooltips.
  select
    sp.reporter_country_code,
    sp.reporter_country_name,
    sp.reporter_region,
    sp.reporter_subregion,
    sp.reporter_continent,
    sp.period as year_month_key,
    sp.year_month,
    sp.month_start_date,
    sp.year,
    format_date('%b', sp.month_start_date) as month_name_short,
    format_date('%b %Y', sp.month_start_date) as month_label,
    sp.total_trade_value_usd,
    sp.import_trade_value_usd,
    sp.export_trade_value_usd,
    sp.trade_balance_usd,
    sp.total_net_weight_kg,
    sp.total_gross_weight_kg,
    sp.previous_month_trade_value_usd,
    sp.total_trade_value_usd / 1000000000.0 as total_trade_value_billion,
    case
      when sp.total_trade_value_usd is null then null
      when abs(sp.total_trade_value_usd) >= 1000000000000 then concat(format('%.2f', sp.total_trade_value_usd / 1000000000000.0), ' trillion')
      when abs(sp.total_trade_value_usd) >= 1000000000 then concat(format('%.2f', sp.total_trade_value_usd / 1000000000.0), ' billion')
      when abs(sp.total_trade_value_usd) >= 1000000 then concat(format('%.2f', sp.total_trade_value_usd / 1000000.0), ' million')
      when abs(sp.total_trade_value_usd) >= 1000 then concat(format('%.1f', sp.total_trade_value_usd / 1000.0), ' thousand')
      else format('%.0f', sp.total_trade_value_usd)
    end as total_trade_value_label,
    case
      when sp.trade_balance_usd is null then null
      when abs(sp.trade_balance_usd) >= 1000000000000 then concat(format('%.2f', sp.trade_balance_usd / 1000000000000.0), ' trillion')
      when abs(sp.trade_balance_usd) >= 1000000000 then concat(format('%.2f', sp.trade_balance_usd / 1000000000.0), ' billion')
      when abs(sp.trade_balance_usd) >= 1000000 then concat(format('%.2f', sp.trade_balance_usd / 1000000.0), ' million')
      when abs(sp.trade_balance_usd) >= 1000 then concat(format('%.1f', sp.trade_balance_usd / 1000.0), ' thousand')
      else format('%.0f', sp.trade_balance_usd)
    end as trade_balance_label,
    sp.import_trade_share_pct,
    format('%.1f%%', sp.import_trade_share_pct) as import_trade_share_label,
    sp.export_trade_share_pct,
    format('%.1f%%', sp.export_trade_share_pct) as export_trade_share_label,
    sp.trade_value_mom_change_pct,
    format('%.1f%%', sp.trade_value_mom_change_pct) as trade_value_mom_change_label,
    sp.trade_balance_direction,
    sp.trade_orientation,
    sp.reporter_rank_by_trade_value
  from scored_profile as sp
)

select
  reporter_country_code,
  reporter_country_name,
  reporter_region,
  reporter_subregion,
  reporter_continent,
  year_month_key,
  year_month,
  month_start_date,
  year,
  month_name_short,
  month_label,
  total_trade_value_usd,
  import_trade_value_usd,
  export_trade_value_usd,
  trade_balance_usd,
  total_net_weight_kg,
  total_gross_weight_kg,
  previous_month_trade_value_usd,
  total_trade_value_billion,
  total_trade_value_label,
  trade_balance_label,
  import_trade_share_pct,
  import_trade_share_label,
  export_trade_share_pct,
  export_trade_share_label,
  trade_value_mom_change_pct,
  trade_value_mom_change_label,
  trade_balance_direction,
  trade_orientation,
  reporter_rank_by_trade_value
from final