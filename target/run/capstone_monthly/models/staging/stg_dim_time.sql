
  
  create view "analytics"."analytics_staging"."stg_dim_time__dbt_tmp" as (
    select
  cast(period as integer) as period,
  cast(year as integer) as year,
  cast(month as integer) as month,
  cast(quarter as integer) as quarter,
  year_month,
  cast(date as date) as month_start_date
from "analytics"."raw"."dim_time"
  );
