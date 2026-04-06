-- Test: composite risk score should remain within 0-100 bounds.

select
  reporter_country_code,
  year_month_key,
  composite_risk_score
from {{ ref('mart_country_risk_summary') }}
where composite_risk_score < 0
   or composite_risk_score > 100
