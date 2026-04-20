select
  iso3,
  country_name
from {{ ref('dim_country') }}
where iso3 != upper(trim(iso3))
   or country_name != trim(country_name)
   or iso3 in ('ROM', 'UK', 'US', 'SA', 'CN')
   or lower(country_name) in (
      'türkiye',
      'turkie',
      'turkish republic',
      'russian federation',
      'holland',
      'republic of south africa',
      'u.s.a.',
      'united states of america',
      'metropolitan france'
   )
