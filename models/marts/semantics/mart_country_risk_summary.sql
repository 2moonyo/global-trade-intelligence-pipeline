-- Grain: one row per reporter_country_code + period.
-- Purpose: consolidated monthly risk summary combining partner, commodity, chokepoint, and event signals.

with trade_profile_month as (
  -- Reporter-month profile is the anchor grain for the final summary.
  select
    ctp.reporter_country_code,
    ctp.reporter_country_name,
    ctp.reporter_region,
    ctp.reporter_subregion,
    ctp.reporter_continent,
    ctp.year_month_key,
    ctp.year_month,
    ctp.month_start_date,
    ctp.year,
    ctp.month_name_short,
    ctp.month_label,
    ctp.total_trade_value_usd,
    ctp.total_trade_value_billion,
    ctp.total_trade_value_label,
    ctp.trade_value_mom_change_pct,
    ctp.trade_value_mom_change_label,
    ctp.trade_orientation
  from {{ ref('mart_country_trade_profile') }} as ctp
),
partner_concentration_month as (
  -- Partner concentration uses HHI from partner share percentages at reporter-month grain.
  select
    cpd.reporter_country_code,
    cpd.year_month_key,
    cpd.year_month,
    sum(power(coalesce(cpd.partner_trade_share_pct, 0) / 100.0, 2)) as partner_hhi_index,
    max(cpd.partner_trade_share_pct) as top_partner_share_pct
  from {{ ref('mart_country_partner_dependency') }} as cpd
  group by 1, 2, 3
),
commodity_concentration_month as (
  -- Commodity concentration uses HHI from commodity share percentages at reporter-month grain.
  select
    ccd.reporter_country_code,
    ccd.year_month_key,
    ccd.year_month,
    sum(power(coalesce(ccd.commodity_trade_share_pct, 0) / 100.0, 2)) as commodity_hhi_index,
    max(ccd.commodity_trade_share_pct) as top_commodity_share_pct
  from {{ ref('mart_country_commodity_dependency') }} as ccd
  group by 1, 2, 3
),
chokepoint_dependency_month as (
  -- Chokepoint exposure aggregates high-level route vulnerability at reporter-month grain.
  select
    cce.reporter_country_code,
    cce.year_month_key,
    cce.year_month,
    least(100.0, sum(coalesce(cce.chokepoint_exposure_pct, 0))) as total_chokepoint_exposure_pct,
    max(cce.chokepoint_exposure_pct) as top_chokepoint_exposure_pct,
    avg(cce.stress_index_weighted_rolling_6m) as avg_stress_index_weighted_rolling_6m,
    sum(coalesce(cce.active_event_count, 0)) as total_active_event_signals
  from {{ ref('mart_country_chokepoint_exposure') }} as cce
  group by 1, 2, 3
),
event_pressure_month as (
  -- Event pressure captures how many active events materially intersect reporter exposure.
  select
    cei.reporter_country_code,
    cei.year_month_key,
    cei.year_month,
    count(distinct case when cei.is_event_active and cei.event_exposure_pct > 0 then cei.event_id end) as active_exposed_event_count,
    max(cei.event_exposure_pct) as max_event_exposure_pct
  from {{ ref('mart_country_event_impact') }} as cei
  group by 1, 2, 3
),
risk_components as (
  -- Join concentration and exposure components onto the trade profile anchor grain.
  select
    tpm.reporter_country_code,
    tpm.reporter_country_name,
    tpm.reporter_region,
    tpm.reporter_subregion,
    tpm.reporter_continent,
    tpm.year_month_key,
    tpm.year_month,
    tpm.month_start_date,
    tpm.year,
    tpm.month_name_short,
    tpm.month_label,
    tpm.total_trade_value_usd,
    tpm.total_trade_value_billion,
    tpm.total_trade_value_label,
    tpm.trade_value_mom_change_pct,
    tpm.trade_value_mom_change_label,
    tpm.trade_orientation,
    pcm.partner_hhi_index,
    pcm.top_partner_share_pct,
    ccm.commodity_hhi_index,
    ccm.top_commodity_share_pct,
    cdm.total_chokepoint_exposure_pct,
    cdm.top_chokepoint_exposure_pct,
    cdm.avg_stress_index_weighted_rolling_6m,
    cdm.total_active_event_signals,
    epm.active_exposed_event_count,
    epm.max_event_exposure_pct
  from trade_profile_month as tpm
  left join partner_concentration_month as pcm
    on tpm.reporter_country_code = pcm.reporter_country_code
   and tpm.year_month_key = pcm.year_month_key
   and tpm.year_month = pcm.year_month
  left join commodity_concentration_month as ccm
    on tpm.reporter_country_code = ccm.reporter_country_code
   and tpm.year_month_key = ccm.year_month_key
   and tpm.year_month = ccm.year_month
  left join chokepoint_dependency_month as cdm
    on tpm.reporter_country_code = cdm.reporter_country_code
   and tpm.year_month_key = cdm.year_month_key
   and tpm.year_month = cdm.year_month
  left join event_pressure_month as epm
    on tpm.reporter_country_code = epm.reporter_country_code
   and tpm.year_month_key = epm.year_month_key
   and tpm.year_month = epm.year_month
),
scored_summary as (
  -- Composite scoring keeps structural and observed components separate but comparable.
  select
    rc.reporter_country_code,
    rc.reporter_country_name,
    rc.reporter_region,
    rc.reporter_subregion,
    rc.reporter_continent,
    rc.year_month_key,
    rc.year_month,
    rc.month_start_date,
    rc.year,
    rc.month_name_short,
    rc.month_label,
    rc.total_trade_value_usd,
    rc.total_trade_value_billion,
    rc.total_trade_value_label,
    rc.trade_value_mom_change_pct,
    rc.trade_value_mom_change_label,
    rc.trade_orientation,
    rc.partner_hhi_index,
    rc.top_partner_share_pct,
    rc.commodity_hhi_index,
    rc.top_commodity_share_pct,
    rc.total_chokepoint_exposure_pct,
    rc.top_chokepoint_exposure_pct,
    rc.avg_stress_index_weighted_rolling_6m,
    rc.total_active_event_signals,
    rc.active_exposed_event_count,
    rc.max_event_exposure_pct,
    coalesce(rc.partner_hhi_index, 0) * 100 as partner_concentration_index,
    coalesce(rc.commodity_hhi_index, 0) * 100 as commodity_concentration_index,
    (
      coalesce(rc.total_chokepoint_exposure_pct, 0) * 0.45
      + (coalesce(rc.partner_hhi_index, 0) * 100) * 0.25
      + (coalesce(rc.commodity_hhi_index, 0) * 100) * 0.20
      + least(100.0, coalesce(rc.active_exposed_event_count, 0) * 10.0) * 0.10
    ) as composite_risk_score,
    case
      when coalesce(rc.total_chokepoint_exposure_pct, 0) >= 35 then 'very_high'
      when coalesce(rc.total_chokepoint_exposure_pct, 0) >= 20 then 'high'
      when coalesce(rc.total_chokepoint_exposure_pct, 0) >= 10 then 'moderate'
      when coalesce(rc.total_chokepoint_exposure_pct, 0) >= 5 then 'low'
      else 'very_low'
    end as dependency_level
  from risk_components as rc
),
final as (
  -- Final semantic projection includes numeric risk score and dashboard labels.
  select
    ss.reporter_country_code,
    ss.reporter_country_name,
    ss.reporter_region,
    ss.reporter_subregion,
    ss.reporter_continent,
    ss.year_month_key,
    ss.year_month,
    ss.month_start_date,
    ss.year,
    ss.month_name_short,
    ss.month_label,
    ss.total_trade_value_usd,
    ss.total_trade_value_billion,
    ss.total_trade_value_label,
    ss.trade_value_mom_change_pct,
    ss.trade_value_mom_change_label,
    ss.trade_orientation,
    ss.partner_hhi_index,
    ss.partner_concentration_index,
    ss.top_partner_share_pct,
    ss.commodity_hhi_index,
    ss.commodity_concentration_index,
    ss.top_commodity_share_pct,
    ss.total_chokepoint_exposure_pct,
    format('%.1f%%', ss.total_chokepoint_exposure_pct) as total_chokepoint_exposure_label,
    ss.top_chokepoint_exposure_pct,
    ss.avg_stress_index_weighted_rolling_6m,
    ss.total_active_event_signals,
    ss.active_exposed_event_count,
    ss.max_event_exposure_pct,
    ss.composite_risk_score,
    format('%.1f', ss.composite_risk_score) as composite_risk_score_label,
    ss.dependency_level,
    case
      when ss.composite_risk_score >= 45 then 'high'
      when ss.composite_risk_score >= 25 then 'medium'
      else 'low'
    end as risk_level
  from scored_summary as ss
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
  total_trade_value_billion,
  total_trade_value_label,
  trade_value_mom_change_pct,
  trade_value_mom_change_label,
  trade_orientation,
  partner_hhi_index,
  partner_concentration_index,
  top_partner_share_pct,
  commodity_hhi_index,
  commodity_concentration_index,
  top_commodity_share_pct,
  total_chokepoint_exposure_pct,
  total_chokepoint_exposure_label,
  top_chokepoint_exposure_pct,
  avg_stress_index_weighted_rolling_6m,
  total_active_event_signals,
  active_exposed_event_count,
  max_event_exposure_pct,
  composite_risk_score,
  composite_risk_score_label,
  dependency_level,
  risk_level
from final
