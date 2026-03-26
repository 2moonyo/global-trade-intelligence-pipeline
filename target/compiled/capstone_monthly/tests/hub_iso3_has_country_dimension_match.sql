-- Fails when non-null hub ISO3 values do not resolve to the country dimension.
select
  h.hub_iso3,
  count(*) as row_count
from "analytics"."analytics_marts"."fct_reporter_partner_commodity_hub_month" as h
left join "analytics"."analytics_staging"."stg_dim_country" as c
  on h.hub_iso3 = c.iso3
where h.hub_iso3 is not null
  and c.iso3 is null
group by 1