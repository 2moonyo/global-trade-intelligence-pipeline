select distinct
  hm.chokepoint_id,
  hm.chokepoint_name
from {{ ref('mart_chokepoint_monthly_hotspot_map') }} as hm
left join {{ ref('dim_chokepoint') }} as dc
  on hm.chokepoint_id = dc.chokepoint_id
where dc.chokepoint_id is null
