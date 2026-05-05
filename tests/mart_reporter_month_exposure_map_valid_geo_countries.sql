select
  m.reporter_iso2,
  m.reporter_iso3,
  m.reporter_country_name,
  m.reporter_country_name_looker,
  dc.is_country_group,
  dc.is_country_map_eligible
from {{ ref('mart_reporter_month_exposure_map') }} as m
left join {{ ref('dim_country') }} as dc
  on m.reporter_iso3 = dc.iso3
where dc.iso3 is null
   or not coalesce(dc.is_country_map_eligible, false)
   or m.reporter_iso2 is null
   or m.reporter_country_name_looker is null
   or m.reporter_iso2 != upper(trim(m.reporter_iso2))
   or m.reporter_iso3 != upper(trim(m.reporter_iso3))
   or m.reporter_country_name != trim(m.reporter_country_name)
   or m.reporter_country_name_looker != trim(m.reporter_country_name_looker)
   or m.reporter_iso3 in ('EU', 'EUR', 'WLD', 'W00', 'A79', 'E19', 'F19', 'S19', 'X1', 'XX', '_X')
   or lower(m.reporter_country_name) in (
      'european union',
      'world',
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
   or lower(m.reporter_country_name_looker) in (
      'türkiye',
      'russian federation',
      'united states of america',
      'viet nam',
      'rep. of korea',
      'dem. rep. of the congo',
      'china, hong kong sar',
      'china, macao sar'
   )
