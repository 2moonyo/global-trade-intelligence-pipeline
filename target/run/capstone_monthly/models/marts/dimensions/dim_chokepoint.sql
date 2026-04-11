
  
    

    create or replace table `chokepoint-capfractal`.`analytics_marts`.`dim_chokepoint`
      
    
    

    
    OPTIONS()
    as (
      with route_chokepoints as (
  select distinct
    
    to_hex(md5(cast(lower(trim(main_chokepoint)) as string)))
   as chokepoint_id,
    main_chokepoint as chokepoint_name
  from `chokepoint-capfractal`.`analytics_marts`.`fct_reporter_partner_commodity_route_month`
  where main_chokepoint is not null
),
raw_chokepoints as (
  select
    chokepoint_id,
    chokepoint_name,
    chokepoint_kind,
    longitude,
    latitude,
    zone_of_influence_radius_m,
    chokepoint_point_wkb,
    zone_of_influence_wkb,
    chokepoint_point_geog,
    zone_of_influence_geog
  from `chokepoint-capfractal`.`analytics_staging`.`stg_dim_chokepoint`
),
event_chokepoints as (
  select distinct
    
    to_hex(md5(cast(lower(trim(chokepoint_name)) as string)))
   as chokepoint_id,
    chokepoint_name
  from `chokepoint-capfractal`.`analytics_staging`.`stg_chokepoint_bridge`
  where chokepoint_name is not null
),
portwatch_chokepoints as (
  select distinct
    chokepoint_id,
    chokepoint_name,
    portwatch_source_chokepoint_id,
    tanker_share,
    container_share,
    dry_bulk_share
  from `chokepoint-capfractal`.`analytics_staging`.`stg_portwatch_stress_metrics`
  where chokepoint_name is not null
),
base as (
  select
    c.chokepoint_id,
    c.chokepoint_name,
    max(p.portwatch_source_chokepoint_id) as portwatch_source_chokepoint_id,
    max(p.tanker_share) as latest_tanker_share,
    max(p.container_share) as latest_container_share,
    max(p.dry_bulk_share) as latest_dry_bulk_share
  from (
    select chokepoint_id, chokepoint_name from route_chokepoints
    union distinct
    select chokepoint_id, chokepoint_name from event_chokepoints
    union distinct
    select chokepoint_id, chokepoint_name from portwatch_chokepoints
  ) as c
  left join portwatch_chokepoints as p
    on c.chokepoint_id = p.chokepoint_id
  group by 1, 2
)

select
  b.chokepoint_id,
  coalesce(rc.chokepoint_name, b.chokepoint_name) as chokepoint_name,
  rc.chokepoint_kind,
  rc.longitude,
  rc.latitude,
  rc.zone_of_influence_radius_m,
  rc.chokepoint_point_wkb,
  rc.zone_of_influence_wkb,
  rc.chokepoint_point_geog,
  rc.zone_of_influence_geog,
  b.portwatch_source_chokepoint_id,
  b.latest_tanker_share,
  b.latest_container_share,
  b.latest_dry_bulk_share
from base as b
left join raw_chokepoints as rc
  on b.chokepoint_id = rc.chokepoint_id
    );
  