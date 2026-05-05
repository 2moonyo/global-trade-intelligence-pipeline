select
  iso3,
  bloc_code,
  notes
from {{ ref('dim_country_bloc_membership') }}
where bloc_code = 'WESTERN_ALIGNED_PROXY'
  and coalesce(notes, '') <> 'Analytical grouping for dashboard comparison only; not an official legal or political classification.'
