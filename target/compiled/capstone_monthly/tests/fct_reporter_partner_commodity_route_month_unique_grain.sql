-- Fails when route-enriched fact duplicates canonical grain rows.
with duplicate_grain as (
  select
    reporter_iso3,
    partner_iso3,
    cmd_code,
    period,
    year_month,
    trade_flow,
    count(*) as row_count
  from "analytics"."analytics_marts"."fct_reporter_partner_commodity_route_month"
  group by 1, 2, 3, 4, 5, 6
  having count(*) > 1
)

select *
from duplicate_grain