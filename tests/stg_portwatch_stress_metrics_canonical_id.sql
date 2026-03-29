select *
from {{ ref('stg_portwatch_stress_metrics') }}
where chokepoint_id != md5(lower(trim(chokepoint_name)))
