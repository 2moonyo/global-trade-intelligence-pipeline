import duckdb


def run_query(con: duckdb.DuckDBPyConnection, title: str, sql: str) -> None:
    print(f"\n=== {title} ===")
    print(con.sql(sql).df().to_string(index=False))


def main() -> None:
    con = duckdb.connect("warehouse/analytics.duckdb")

    run_query(
        con,
        "row_counts",
        """
        select 'fct_core' as table_name, count(*) as row_count from analytics_marts.fct_reporter_partner_commodity_month
        union all
        select 'fct_route', count(*) from analytics_marts.fct_reporter_partner_commodity_route_month
        union all
        select 'fct_hub', count(*) from analytics_marts.fct_reporter_partner_commodity_hub_month
        union all
        select 'mart_hub_dependency_month', count(*) from analytics_marts.mart_hub_dependency_month
        """,
    )

    run_query(
        con,
        "duplicate_counts",
        """
        with core_dupes as (
          select count(*) as cnt from (
            select reporter_iso3, partner_iso3, cmd_code, period, year_month, trade_flow
            from analytics_marts.fct_reporter_partner_commodity_month
            group by 1,2,3,4,5,6
            having count(*) > 1
          )
        ),
        route_dupes as (
          select count(*) as cnt from (
            select reporter_iso3, partner_iso3, cmd_code, period, year_month, trade_flow
            from analytics_marts.fct_reporter_partner_commodity_route_month
            group by 1,2,3,4,5,6
            having count(*) > 1
          )
        ),
        hub_dupes as (
          select count(*) as cnt from (
            select reporter_iso3, partner_iso3, coalesce(partner2_iso3, '__NULL__'), cmd_code, period, year_month, trade_flow
            from analytics_marts.fct_reporter_partner_commodity_hub_month
            group by 1,2,3,4,5,6,7
            having count(*) > 1
          )
        )
        select 'core_duplicate_groups' as check_name, cnt from core_dupes
        union all
        select 'route_duplicate_groups', cnt from route_dupes
        union all
        select 'hub_duplicate_groups', cnt from hub_dupes
        """,
    )

    run_query(
        con,
        "hub_id_connections",
        """
        select
          count(*) filter (where hub_iso3 is not null) as hub_rows_non_null,
          count(*) filter (where hub_iso3 is not null and c.iso3 is null) as hub_rows_unmatched_country_dim
        from analytics_marts.fct_reporter_partner_commodity_hub_month h
        left join analytics_staging.stg_dim_country c
          on h.hub_iso3 = c.iso3
        """,
    )

    run_query(
        con,
        "core_vs_hub_reconciliation",
        """
        with core as (
          select
            reporter_iso3, partner_iso3, cmd_code, period, year_month, trade_flow,
            sum(trade_value_usd) as core_trade_value_usd
          from analytics_marts.fct_reporter_partner_commodity_month
          group by 1,2,3,4,5,6
        ),
        hub as (
          select
            reporter_iso3, partner_iso3, cmd_code, period, year_month, trade_flow,
            sum(trade_value_usd) as hub_trade_value_usd
          from analytics_marts.fct_reporter_partner_commodity_hub_month
          group by 1,2,3,4,5,6
        )
        select
          count(*) as key_count,
          sum(abs(core.core_trade_value_usd - hub.hub_trade_value_usd)) as sum_abs_diff,
          max(abs(core.core_trade_value_usd - hub.hub_trade_value_usd)) as max_abs_diff,
          count(*) filter (where abs(core.core_trade_value_usd - hub.hub_trade_value_usd) > 1e-6) as keys_with_diff
        from core
        inner join hub
          using (reporter_iso3, partner_iso3, cmd_code, period, year_month, trade_flow)
        """,
    )

    run_query(
        con,
        "route_boolean_consistency",
        """
        with expected as (
          select
            reporter_iso3, partner_iso3, cmd_code, period, year_month, trade_flow,
            bool_or(has_partner2_hub) as expected_has_hub
          from analytics_marts.fct_reporter_partner_commodity_hub_month
          group by 1,2,3,4,5,6
        )
        select
          count(*) as route_rows,
          count(*) filter (where r.has_associated_hub_route <> coalesce(e.expected_has_hub, false)) as mismatched_rows
        from analytics_marts.fct_reporter_partner_commodity_route_month r
        left join expected e
          using (reporter_iso3, partner_iso3, cmd_code, period, year_month, trade_flow)
        """,
    )

    run_query(
        con,
        "top_hub_sample",
        """
        select
          hub_iso3,
          hub_country_name,
          year_month,
          route_confidence_score,
          round(hub_routed_trade_value_usd, 2) as hub_routed_trade_value_usd,
          round(hub_trade_share_of_global_month * 100, 4) as hub_trade_share_pct
        from analytics_marts.mart_hub_dependency_month
        order by hub_routed_trade_value_usd desc
        limit 10
        """,
    )

    con.close()


if __name__ == "__main__":
    main()
