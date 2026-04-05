with route_chokepoints as (
  select distinct
    {{ hash_text('lower(trim(main_chokepoint))') }} as chokepoint_id,
    main_chokepoint as chokepoint_name
  from {{ ref('fct_reporter_partner_commodity_route_month') }}
  where main_chokepoint is not null
),
event_chokepoints as (
  select distinct
    {{ hash_text('lower(trim(chokepoint_name))') }} as chokepoint_id,
    chokepoint_name
  from {{ ref('stg_chokepoint_bridge') }}
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
  from {{ ref('stg_portwatch_stress_metrics') }}
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
  chokepoint_id,
  chokepoint_name,
  portwatch_source_chokepoint_id,
  latest_tanker_share,
  latest_container_share,
  latest_dry_bulk_share
from base
