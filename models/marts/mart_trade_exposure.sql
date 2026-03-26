-- Grain: one row per reporter_iso3 + chokepoint_id + year_month + route_confidence_score.
-- Purpose: reusable trade exposure mart for event-impact country-coverage joins.

with routed_trade as (
  select
    reporter_iso3,
    partner_iso3,
    period,
    year_month,
    main_chokepoint,
    route_confidence_score,
    trade_value_usd
  from {{ ref('fct_reporter_partner_commodity_route_month') }}
  where main_chokepoint is not null
    and coalesce(is_maritime_routed, false)
),
reporter_month_totals as (
  select
    reporter_iso3,
    period,
    year_month,
    sum(trade_value_usd) as reporter_month_trade_value_usd
  from {{ ref('fct_reporter_partner_commodity_month') }}
  group by 1, 2, 3
),
reporter_chokepoint_confidence as (
  select
    rt.reporter_iso3,
    md5(lower(trim(rt.main_chokepoint))) as chokepoint_id,
    rt.main_chokepoint as chokepoint_name,
    rt.period,
    rt.year_month,
    rt.route_confidence_score,
    sum(rt.trade_value_usd) as chokepoint_trade_value_usd,
    count(distinct rt.partner_iso3) as route_pair_count
  from routed_trade as rt
  group by 1, 2, 3, 4, 5, 6
)

select
  rcc.reporter_iso3,
  c.country_name as reporter_country_name,
  rcc.chokepoint_id,
  rcc.chokepoint_name,
  rcc.period,
  rcc.year_month,
  rcc.route_confidence_score,
  rcc.route_pair_count,
  rcc.chokepoint_trade_value_usd,
  rmt.reporter_month_trade_value_usd,
  case
    when rmt.reporter_month_trade_value_usd = 0 then null
    else rcc.chokepoint_trade_value_usd / rmt.reporter_month_trade_value_usd
  end as chokepoint_trade_exposure_ratio
from reporter_chokepoint_confidence as rcc
left join reporter_month_totals as rmt
  on rcc.reporter_iso3 = rmt.reporter_iso3
 and rcc.period = rmt.period
 and rcc.year_month = rmt.year_month
left join {{ ref('stg_dim_country') }} as c
  on rcc.reporter_iso3 = c.iso3