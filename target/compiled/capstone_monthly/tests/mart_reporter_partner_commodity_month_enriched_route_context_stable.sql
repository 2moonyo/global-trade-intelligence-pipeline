with route_context as (
  select
    reporter_iso3,
    partner_iso3,
    cmd_code,
    period,
    main_chokepoint as chokepoint_name,
    count(distinct coalesce(route_group, '__NULL__')) as route_group_count,
    count(distinct coalesce(headline_exposure_group, '__NULL__')) as headline_exposure_group_count,
    count(distinct coalesce(route_confidence_score, '__NULL__')) as route_confidence_score_count,
    count(distinct coalesce(route_applicability_status, '__NULL__')) as route_applicability_status_count,
    count(distinct coalesce(cast(used_transshipment_hub as string), '__NULL__')) as used_transshipment_hub_count,
    count(distinct coalesce(hub_iso3, '__NULL__')) as hub_iso3_count
  from `capfractal`.`analytics_marts`.`fct_reporter_partner_commodity_route_month`
  where main_chokepoint is not null
    and coalesce(is_maritime_routed, false)
  group by 1, 2, 3, 4, 5
)

select *
from route_context
where route_group_count > 1
  or headline_exposure_group_count > 1
  or route_confidence_score_count > 1
  or route_applicability_status_count > 1
  or used_transshipment_hub_count > 1
  or hub_iso3_count > 1