select
  upper(trim(reporter_iso3)) as reporter_iso3,
  upper(trim(partner_iso3)) as partner_iso3,
  upper(trim(partner2_iso3)) as partner2_iso3,
  {{ cast_int('row_count') }} as row_count,
  {{ cast_float('trade_value_usd') }} as trade_value_usd,
  cast(has_sea as boolean) as has_sea,
  cast(has_inland_water as boolean) as has_inland_water,
  cast(has_unknown as boolean) as has_unknown,
  cast(has_non_marine as boolean) as has_non_marine,
  mot_codes_seen,
  route_applicability_status
from {{ source('raw', 'route_applicability') }}
