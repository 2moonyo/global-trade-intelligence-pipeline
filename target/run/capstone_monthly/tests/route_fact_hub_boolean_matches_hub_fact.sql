
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  -- Fails when route-fact hub boolean disagrees with existence of hub rows for same canonical keys.
with hub_presence as (
  select
    reporter_iso3,
    partner_iso3,
    cmd_code,
    period,
    year_month,
    trade_flow,
    bool_or(has_partner2_hub) as expected_has_hub
  from "analytics"."analytics_marts"."fct_reporter_partner_commodity_hub_month"
  group by 1, 2, 3, 4, 5, 6
),
route_rows as (
  select
    reporter_iso3,
    partner_iso3,
    cmd_code,
    period,
    year_month,
    trade_flow,
    has_associated_hub_route
  from "analytics"."analytics_marts"."fct_reporter_partner_commodity_route_month"
)

select
  rr.reporter_iso3,
  rr.partner_iso3,
  rr.cmd_code,
  rr.period,
  rr.year_month,
  rr.trade_flow,
  rr.has_associated_hub_route,
  coalesce(hp.expected_has_hub, false) as expected_has_hub
from route_rows as rr
left join hub_presence as hp
  on rr.reporter_iso3 = hp.reporter_iso3
 and rr.partner_iso3 = hp.partner_iso3
 and rr.cmd_code = hp.cmd_code
 and rr.period = hp.period
 and rr.year_month = hp.year_month
 and rr.trade_flow = hp.trade_flow
where rr.has_associated_hub_route <> coalesce(hp.expected_has_hub, false)
  
  
      
    ) dbt_internal_test