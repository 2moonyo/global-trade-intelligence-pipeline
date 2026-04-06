-- Test: mart_country_chokepoint_exposure must be unique at reporter_country_code + chokepoint_id + year_month_key + year_month.

select
  reporter_country_code,
  chokepoint_id,
  year_month_key,
  year_month,
  count(*) as row_count
from `capfractal`.`analytics_marts`.`mart_country_chokepoint_exposure`
group by 1, 2, 3, 4
having count(*) > 1