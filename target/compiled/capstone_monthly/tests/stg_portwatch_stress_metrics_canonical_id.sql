select *
from `fullcap-10111`.`analytics_staging`.`stg_portwatch_stress_metrics`
where chokepoint_id != 
    to_hex(md5(cast(lower(trim(chokepoint_name)) as string)))
  