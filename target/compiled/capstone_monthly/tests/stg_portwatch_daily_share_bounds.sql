select *
from `chokepoint-capfractal`.`analytics_staging`.`stg_portwatch_daily`
where (
    tanker_share is not null
    and (
      tanker_share < 0
      or tanker_share > 1
    )
  )
  or (
    container_share is not null
    and (
      container_share < 0
      or container_share > 1
    )
  )
  or (
    dry_bulk_share is not null
    and (
      dry_bulk_share < 0
      or dry_bulk_share > 1
    )
  )
  or (
    priority_vessel_share is not null
    and (
      priority_vessel_share < 0
      or priority_vessel_share > 1
    )
  )