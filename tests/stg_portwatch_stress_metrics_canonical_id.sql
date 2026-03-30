select *
from {{ ref('stg_portwatch_stress_metrics') }}
where chokepoint_id != {{ hash_text('lower(trim(chokepoint_name))') }}
