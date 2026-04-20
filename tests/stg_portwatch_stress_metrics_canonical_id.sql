select *
from {{ ref('stg_portwatch_stress_metrics') }}
where chokepoint_id != {{ canonical_chokepoint_id('chokepoint_name') }}
