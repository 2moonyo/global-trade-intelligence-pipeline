
  
    

    create or replace table `capfractal`.`analytics_marts`.`mart_reporter_month_exposure_map`
      
    
    

    
    OPTIONS()
    as (
      -- Monthly Looker Studio support mart for the Page 4 reporter-country exposure map.
-- Grain: one row per month_start_date + reporter_iso3.

with base_exposure as (
  select
    reporter_iso3,
    period,
    year_month,
    month_start_date,
    chokepoint_id,
    chokepoint_name,
    route_pair_count,
    chokepoint_trade_value_usd,
    reporter_month_trade_value_usd,
    chokepoint_trade_exposure_ratio,
    stress_index_weighted,
    active_event_count
  from `capfractal`.`analytics_marts`.`mart_reporter_month_chokepoint_exposure`
),
high_medium_exposure as (
  select
    reporter_iso3,
    period,
    year_month,
    sum(chokepoint_trade_value_usd) as high_medium_chokepoint_exposed_trade_value_usd,
    sum(chokepoint_trade_exposure_ratio) as high_medium_chokepoint_exposed_trade_share
  from `capfractal`.`analytics_marts`.`mart_trade_exposure`
  where route_confidence_score in ('HIGH', 'MEDIUM')
  group by 1, 2, 3
),
aggregated_country as (
  select
    reporter_iso3,
    period,
    year_month,
    month_start_date,
    max(reporter_month_trade_value_usd) as reporter_month_trade_value_usd,
    sum(chokepoint_trade_value_usd) as total_chokepoint_exposed_trade_value_usd,
    sum(chokepoint_trade_exposure_ratio) as total_chokepoint_exposed_trade_share,
    count(distinct chokepoint_id) as exposed_chokepoint_count,
    sum(route_pair_count) as exposed_route_pair_count,
    count(distinct case when active_event_count > 0 then chokepoint_id end) as event_impacted_exposed_chokepoint_count,
    max(stress_index_weighted) as max_exposed_chokepoint_stress_index_weighted,
    case
      when sum(case when stress_index_weighted is not null then chokepoint_trade_value_usd else 0 end) = 0 then null
      else sum(case when stress_index_weighted is not null then chokepoint_trade_value_usd * stress_index_weighted else 0 end)
        / sum(case when stress_index_weighted is not null then chokepoint_trade_value_usd else 0 end)
    end as trade_value_weighted_stress_index_weighted
  from base_exposure
  group by 1, 2, 3, 4
),
top_chokepoint_candidates as (
  select
    reporter_iso3,
    period,
    year_month,
    month_start_date,
    chokepoint_id,
    chokepoint_name,
    chokepoint_trade_value_usd,
    chokepoint_trade_exposure_ratio,
    stress_index_weighted,
    row_number() over (
      partition by reporter_iso3, period
      order by
        chokepoint_trade_value_usd desc,
        chokepoint_trade_exposure_ratio desc,
        chokepoint_name
    ) as exposure_rank_in_country_month
  from base_exposure
),
top_chokepoint as (
  select
    reporter_iso3,
    period,
    year_month,
    month_start_date,
    chokepoint_id as top_exposed_chokepoint_id,
    chokepoint_name as top_exposed_chokepoint_name,
    chokepoint_trade_value_usd as top_exposed_chokepoint_trade_value_usd,
    chokepoint_trade_exposure_ratio as top_exposed_chokepoint_trade_share_of_reporter_total,
    stress_index_weighted as top_exposed_chokepoint_stress_index_weighted
  from top_chokepoint_candidates
  where exposure_rank_in_country_month = 1
),
global_bounds as (
  select max(month_start_date) as latest_month_start_date
  from aggregated_country
)

select
  ac.reporter_iso3,
  dc.country_name as reporter_country_name,
  dc.region as reporter_region,
  dc.subregion as reporter_subregion,
  dc.continent as reporter_continent,
  dc.is_eu as reporter_is_eu,
  dc.is_oecd as reporter_is_oecd,
  ac.period,
  ac.year_month,
  ac.month_start_date,
  format_date('%b %Y', ac.month_start_date) as month_label,
  ac.reporter_month_trade_value_usd,
  ac.total_chokepoint_exposed_trade_value_usd,
  ac.total_chokepoint_exposed_trade_share,
  coalesce(hm.high_medium_chokepoint_exposed_trade_value_usd, 0) as high_medium_chokepoint_exposed_trade_value_usd,
  coalesce(hm.high_medium_chokepoint_exposed_trade_share, 0) as high_medium_chokepoint_exposed_trade_share,
  ac.exposed_chokepoint_count,
  ac.exposed_route_pair_count,
  ac.event_impacted_exposed_chokepoint_count,
  ac.max_exposed_chokepoint_stress_index_weighted,
  ac.trade_value_weighted_stress_index_weighted,
  tc.top_exposed_chokepoint_id,
  tc.top_exposed_chokepoint_name,
  tc.top_exposed_chokepoint_trade_value_usd,
  tc.top_exposed_chokepoint_trade_share_of_reporter_total,
  tc.top_exposed_chokepoint_stress_index_weighted,
  case
    when ac.month_start_date = g.latest_month_start_date then true
    else false
  end as latest_month_flag
from aggregated_country as ac
left join high_medium_exposure as hm
  on ac.reporter_iso3 = hm.reporter_iso3
 and ac.period = hm.period
 and ac.year_month = hm.year_month
left join top_chokepoint as tc
  on ac.reporter_iso3 = tc.reporter_iso3
 and ac.period = tc.period
 and ac.year_month = tc.year_month
left join `capfractal`.`analytics_marts`.`dim_country` as dc
  on ac.reporter_iso3 = dc.iso3
cross join global_bounds as g
    );
  