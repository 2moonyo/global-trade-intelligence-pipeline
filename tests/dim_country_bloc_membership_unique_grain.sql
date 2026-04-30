select
  iso3,
  bloc_code,
  count(*) as row_count
from {{ ref('dim_country_bloc_membership') }}
group by 1, 2
having count(*) > 1
