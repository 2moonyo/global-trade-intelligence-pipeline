-- Looker Studio support mart for the Page 4 reporter-country exposure map.
-- Grain: one row per reporter_iso3, using the reporter's latest available exposure month.

with eligible_reporters as (
  select
    iso3,
    country_name,
    region,
    subregion,
    continent,
    is_eu,
    is_oecd
  from {{ ref('dim_country') }}
  where is_country_map_eligible
),
base_exposure as (
  select
    {{ canonical_country_iso3('reporter_iso3') }} as reporter_iso3,
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
  from {{ ref('mart_reporter_month_chokepoint_exposure') }}
),
high_medium_exposure as (
  select
    {{ canonical_country_iso3('te.reporter_iso3') }} as reporter_iso3,
    te.period,
    te.year_month,
    sum(chokepoint_trade_value_usd) as high_medium_chokepoint_exposed_trade_value_usd,
    sum(chokepoint_trade_exposure_ratio) as high_medium_chokepoint_exposed_trade_share
  from {{ ref('mart_trade_exposure') }} as te
  inner join eligible_reporters as er
    on {{ canonical_country_iso3('te.reporter_iso3') }} = er.iso3
  where te.route_confidence_score in ('HIGH', 'MEDIUM')
  group by 1, 2, 3
),
aggregated_country as (
  select
    be.reporter_iso3,
    be.period,
    be.year_month,
    be.month_start_date,
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
  from base_exposure as be
  inner join eligible_reporters as er
    on be.reporter_iso3 = er.iso3
  group by 1, 2, 3, 4
),
top_chokepoint_candidates as (
  select
    be.reporter_iso3,
    be.period,
    be.year_month,
    be.month_start_date,
    be.chokepoint_id,
    be.chokepoint_name,
    be.chokepoint_trade_value_usd,
    be.chokepoint_trade_exposure_ratio,
    be.stress_index_weighted,
    row_number() over (
      partition by be.reporter_iso3, be.period
      order by
        be.chokepoint_trade_value_usd desc,
        be.chokepoint_trade_exposure_ratio desc,
        be.chokepoint_name
    ) as exposure_rank_in_country_month
  from base_exposure as be
  inner join eligible_reporters as er
    on be.reporter_iso3 = er.iso3
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
country_snapshot as (
  select
    ac.reporter_iso3,
    er.country_name as reporter_country_name,
    er.region as reporter_region,
    er.subregion as reporter_subregion,
    er.continent as reporter_continent,
    er.is_eu as reporter_is_eu,
    er.is_oecd as reporter_is_oecd,
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
    true as latest_month_flag,
    row_number() over (
      partition by ac.reporter_iso3
      order by ac.month_start_date desc, ac.period desc
    ) as country_snapshot_rank
  from aggregated_country as ac
  inner join eligible_reporters as er
    on ac.reporter_iso3 = er.iso3
  left join high_medium_exposure as hm
    on ac.reporter_iso3 = hm.reporter_iso3
   and ac.period = hm.period
   and ac.year_month = hm.year_month
  left join top_chokepoint as tc
    on ac.reporter_iso3 = tc.reporter_iso3
   and ac.period = tc.period
   and ac.year_month = tc.year_month
)

select
  reporter_iso3,
  reporter_country_name,
  reporter_region,
  reporter_subregion,
  reporter_continent,
  reporter_is_eu,
  reporter_is_oecd,
  period,
  year_month,
  month_start_date,
  month_label,
  reporter_month_trade_value_usd,
  total_chokepoint_exposed_trade_value_usd,
  total_chokepoint_exposed_trade_share,
  high_medium_chokepoint_exposed_trade_value_usd,
  high_medium_chokepoint_exposed_trade_share,
  exposed_chokepoint_count,
  exposed_route_pair_count,
  event_impacted_exposed_chokepoint_count,
  max_exposed_chokepoint_stress_index_weighted,
  trade_value_weighted_stress_index_weighted,
  top_exposed_chokepoint_id,
  top_exposed_chokepoint_name,
  top_exposed_chokepoint_trade_value_usd,
  top_exposed_chokepoint_trade_share_of_reporter_total,
  top_exposed_chokepoint_stress_index_weighted,
  latest_month_flag
from country_snapshot
where country_snapshot_rank = 1
