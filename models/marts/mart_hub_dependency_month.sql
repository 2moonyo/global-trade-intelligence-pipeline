-- Grain: one row per hub_iso3 + period + year_month + route_confidence_score.
-- Purpose: isolate transshipment-hub dependency analytics from canonical exposure marts.

with hub_trade as (
  select
    reporter_iso3,
    partner_iso3,
    partner2_iso3,
    period,
    year_month,
    route_confidence_score,
    trade_value_usd
  from {{ ref('fct_reporter_partner_commodity_hub_month') }}
  where has_partner2_hub
    and coalesce(is_maritime_routed, false)
),
monthly_total_trade as (
  select
    period,
    year_month,
    sum(trade_value_usd) as total_trade_value_usd
  from {{ ref('fct_reporter_partner_commodity_month') }}
  group by 1, 2
),
hub_month_confidence as (
  select
    ht.partner2_iso3 as hub_iso3,
    ht.period,
    ht.year_month,
    ht.route_confidence_score,
    sum(ht.trade_value_usd) as hub_routed_trade_value_usd,
    count(distinct ht.reporter_iso3) as reporter_count,
    count(distinct ht.partner_iso3) as partner_count,
    count(distinct ht.reporter_iso3 || '|' || ht.partner_iso3) as reporter_partner_pair_count
  from hub_trade as ht
  group by 1, 2, 3, 4
)

select
  hmc.hub_iso3,
  c.country_name as hub_country_name,
  hmc.period,
  hmc.year_month,
  hmc.route_confidence_score,
  hmc.reporter_count,
  hmc.partner_count,
  hmc.reporter_partner_pair_count,
  hmc.hub_routed_trade_value_usd,
  mtt.total_trade_value_usd,
  case
    when mtt.total_trade_value_usd = 0 then null
    else hmc.hub_routed_trade_value_usd / mtt.total_trade_value_usd
  end as hub_trade_share_of_global_month
from hub_month_confidence as hmc
left join monthly_total_trade as mtt
  on hmc.period = mtt.period
 and hmc.year_month = mtt.year_month
left join {{ ref('stg_dim_country') }} as c
  on hmc.hub_iso3 = c.iso3
