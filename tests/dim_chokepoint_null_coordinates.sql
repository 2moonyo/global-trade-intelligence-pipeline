select
  count(*) as null_coordinate_row_count
from {{ ref('dim_chokepoint') }}
where longitude is null
   or latitude is null
having count(*) > 0
