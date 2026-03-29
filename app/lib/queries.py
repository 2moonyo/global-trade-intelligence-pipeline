from __future__ import annotations

from collections.abc import Sequence

import pandas as pd

from app.lib.config import DISPLAY_TABLE_LIMIT, MARTS_SCHEMA
from app.lib.db import run_query, run_scalar, table_exists


TRADE_SUMMARY_TABLE = f"{MARTS_SCHEMA}.mart_reporter_month_trade_summary"
TRADE_COMMODITY_TABLE = f"{MARTS_SCHEMA}.mart_reporter_commodity_month_trade_summary"
CHOKEPOINT_EXPOSURE_TABLE = f"{MARTS_SCHEMA}.mart_reporter_month_chokepoint_exposure"
TRADE_EXPOSURE_TABLE = f"{MARTS_SCHEMA}.mart_trade_exposure"
ENERGY_TABLE = f"{MARTS_SCHEMA}.mart_reporter_energy_vulnerability"
FACT_TABLE = f"{MARTS_SCHEMA}.fct_reporter_partner_commodity_month"
ROUTE_FACT_TABLE = f"{MARTS_SCHEMA}.fct_reporter_partner_commodity_route_month"
EVENT_IMPACT_TABLE = f"{MARTS_SCHEMA}.mart_event_impact"

PORTWATCH_TABLE = "analytics_staging.stg_portwatch_stress_metrics"
COUNTRY_DIM_TABLE = "analytics_staging.stg_dim_country"
COMMODITY_DIM_TABLE = "analytics_staging.stg_dim_commodity"
EVENT_DIM_TABLE = "analytics_analytics_marts.dim_event"
EVENT_MONTH_TABLE = "analytics_analytics_marts.bridge_event_month"
EVENT_CHOKEPOINT_TABLE = "analytics_analytics_marts.bridge_event_chokepoint"

EXCLUDED_ENTITY_CODES = ("EUR", "W00")


def _month_start_expr(column_name: str) -> str:
    return f"cast(strptime({column_name} || '-01', '%Y-%m-%d') as date)"


def _clean_country_name_expr(column_name: str) -> str:
    return f"regexp_replace({column_name}, ' \\(\\.\\.\\.[0-9]{{4}}\\)$', '')"


def _commodity_label_expr(name_column: str, code_column: str) -> str:
    return f"coalesce({name_column}, {code_column}) || ' (' || {code_column} || ')'"


def _country_entity_condition(code_column: str, region_column: str | None = None) -> str:
    clauses = [
        f"{code_column} not in ('EUR', 'W00', 'XX')",
        f"regexp_full_match({code_column}, '^[A-Z]{{3}}$')",
    ]
    if region_column:
        clauses.append(f"coalesce({region_column}, '') not in ('World', 'Special')")
    return " and ".join(clauses)


def _portwatch_canonical_expr(column_name: str) -> str:
    return f"""
        case
            when {column_name} = 'Bab el-Mandeb Strait' then 'Bab el-Mandeb'
            when {column_name} = 'Strait of Hormuz' then 'Hormuz Strait'
            else {column_name}
        end
    """


def _event_canonical_expr(column_name: str) -> str:
    return f"""
        case
            when {column_name} = 'Bab el-Mandeb Strait' then 'Bab el-Mandeb'
            when {column_name} = 'Strait of Hormuz' then 'Hormuz Strait'
            else {column_name}
        end
    """


def _build_in_clause(column_name: str, values: Sequence[object]) -> tuple[str, list[object]]:
    if not values:
        return "", []
    placeholders = ", ".join(["?"] * len(values))
    return f"{column_name} in ({placeholders})", list(values)


def _append_filter(
    clauses: list[str],
    params: list[object],
    column_name: str,
    values: Sequence[object] | None,
) -> None:
    if not values:
        return
    clause, clause_params = _build_in_clause(column_name, values)
    clauses.append(clause)
    params.extend(clause_params)


def _trade_value_column(filters: dict[str, object]) -> str:
    trade_flow = filters.get("trade_flow", "All trade")
    if trade_flow == "Import":
        return "import_trade_value_usd"
    if trade_flow == "Export":
        return "export_trade_value_usd"
    return "total_trade_value_usd"


def get_trade_flow_label(filters: dict[str, object]) -> str:
    return str(filters.get("trade_flow", "All trade"))


def _summary_filter_clause(
    filters: dict[str, object],
    *,
    reporter_column: str | None = None,
    commodity_column: str | None = None,
    period_column: str | None = "year_month",
) -> tuple[str, list[object]]:
    clauses: list[str] = []
    params: list[object] = []

    if reporter_column:
        clauses.append(f"{reporter_column} not in ('EUR', 'W00')")
        _append_filter(clauses, params, reporter_column, filters.get("reporters", ()))

    if commodity_column:
        _append_filter(clauses, params, commodity_column, filters.get("commodities", ()))

    if period_column and filters.get("start_period") and filters.get("end_period"):
        clauses.append(f"{period_column} between ? and ?")
        params.extend([filters["start_period"], filters["end_period"]])

    return (f"where {' and '.join(clauses)}" if clauses else ""), params


def _fact_filter_clause(
    filters: dict[str, object],
    *,
    reporter_column: str = "f.reporter_iso3",
    partner_column: str = "f.partner_iso3",
    commodity_column: str = "f.cmd_code",
    period_column: str | None = "f.year_month",
    flow_column: str | None = "f.trade_flow",
    include_partners: bool = True,
    include_commodities: bool = True,
    include_period: bool = True,
    include_flow: bool = True,
) -> tuple[str, list[object]]:
    clauses = [
        f"{reporter_column} not in ('EUR', 'W00')",
        f"{partner_column} not in ('EUR', 'W00')",
    ]
    params: list[object] = []

    _append_filter(clauses, params, reporter_column, filters.get("reporters", ()))

    if include_partners:
        _append_filter(clauses, params, partner_column, filters.get("partners", ()))

    if include_commodities:
        _append_filter(clauses, params, commodity_column, filters.get("commodities", ()))

    if include_period and period_column and filters.get("start_period") and filters.get("end_period"):
        clauses.append(f"{period_column} between ? and ?")
        params.extend([filters["start_period"], filters["end_period"]])

    if include_flow and flow_column and filters.get("trade_flow") in ("Import", "Export"):
        clauses.append(f"{flow_column} = ?")
        params.append(filters["trade_flow"])

    return f"where {' and '.join(clauses)}", params


def _chokepoint_filter_clause(
    filters: dict[str, object],
    *,
    chokepoint_column: str,
    period_column: str | None = "year_month",
) -> tuple[str, list[object]]:
    clauses: list[str] = []
    params: list[object] = []

    _append_filter(clauses, params, chokepoint_column, filters.get("chokepoints", ()))

    if period_column and filters.get("start_period") and filters.get("end_period"):
        clauses.append(f"{period_column} between ? and ?")
        params.extend([filters["start_period"], filters["end_period"]])

    return (f"where {' and '.join(clauses)}" if clauses else ""), params


def _reporter_name_subquery() -> str:
    return f"""
        (
            select distinct
                reporter_iso3,
                {_clean_country_name_expr('reporter_country_name')} as reporter_country_name
            from {TRADE_SUMMARY_TABLE}
        )
    """


def _dataset_reporter_subquery() -> str:
    return f"""
        (
            select distinct reporter_iso3
            from {TRADE_SUMMARY_TABLE}
            where reporter_iso3 not in ('EUR', 'W00')
        )
    """


def _energy_non_null_condition() -> str:
    return "indicator_value is not null and not isnan(indicator_value)"


def get_filter_options() -> dict[str, object]:
    reporters_df = pd.DataFrame(columns=["value", "label"])
    if table_exists(TRADE_SUMMARY_TABLE):
        reporters_df = run_query(
            f"""
            select distinct
                t.reporter_iso3 as value,
                {_clean_country_name_expr('t.reporter_country_name')} || ' (' || t.reporter_iso3 || ')' as label
            from {TRADE_SUMMARY_TABLE} t
            where t.reporter_iso3 not in ('EUR', 'W00')
            order by 2
            """
        )

    partners_df = pd.DataFrame(columns=["value", "label"])
    if table_exists(FACT_TABLE):
        partner_name_expr = "f.partner_iso3"
        partner_condition = _country_entity_condition("f.partner_iso3")
        join_sql = ""
        if table_exists(COUNTRY_DIM_TABLE):
            partner_name_expr = _clean_country_name_expr("coalesce(c.country_name, f.partner_iso3)")
            partner_condition = _country_entity_condition("f.partner_iso3", "c.region")
            join_sql = f"""
                left join {COUNTRY_DIM_TABLE} c
                    on f.partner_iso3 = c.iso3
            """

        partners_df = run_query(
            f"""
            select distinct
                f.partner_iso3 as value,
                {partner_name_expr} || ' (' || f.partner_iso3 || ')' as label
            from {FACT_TABLE} f
            {join_sql}
            where {partner_condition}
            order by 2
            """
        )

    commodities_df = pd.DataFrame(columns=["value", "label"])
    if table_exists(TRADE_COMMODITY_TABLE):
        commodities_df = run_query(
            f"""
            select distinct
                cmd_code as value,
                {_commodity_label_expr('commodity_name', 'cmd_code')} as label
            from {TRADE_COMMODITY_TABLE}
            where cmd_code is not null
            order by 2
            """
        )
    elif table_exists(FACT_TABLE):
        commodities_df = run_query(
            f"""
            select distinct
                cmd_code as value,
                cmd_code as label
            from {FACT_TABLE}
            where cmd_code is not null
            order by 2
            """
        )

    chokepoint_frames: list[pd.DataFrame] = []
    if table_exists(TRADE_EXPOSURE_TABLE):
        chokepoint_frames.append(
            run_query(
                f"""
                select distinct chokepoint_name as value, chokepoint_name as label
                from {TRADE_EXPOSURE_TABLE}
                where chokepoint_name is not null
                """
            )
        )
    if table_exists(PORTWATCH_TABLE):
        chokepoint_frames.append(
            run_query(
                f"""
                select distinct
                    {_portwatch_canonical_expr('chokepoint_name')} as value,
                    {_portwatch_canonical_expr('chokepoint_name')} as label
                from {PORTWATCH_TABLE}
                where chokepoint_name is not null
                """
            )
        )
    if table_exists(EVENT_CHOKEPOINT_TABLE):
        chokepoint_frames.append(
            run_query(
                f"""
                select distinct
                    {_event_canonical_expr('chokepoint_name')} as value,
                    {_event_canonical_expr('chokepoint_name')} as label
                from {EVENT_CHOKEPOINT_TABLE}
                where chokepoint_name is not null
                """
            )
        )
    chokepoints_df = (
        pd.concat(chokepoint_frames, ignore_index=True).drop_duplicates().sort_values("label")
        if chokepoint_frames
        else pd.DataFrame(columns=["value", "label"])
    )

    all_periods: set[str] = set()
    for table_name in (TRADE_SUMMARY_TABLE, PORTWATCH_TABLE, EVENT_MONTH_TABLE):
        if table_exists(table_name):
            period_df = run_query(f"select distinct year_month from {table_name} where year_month is not null")
            all_periods.update(period_df["year_month"].tolist())

    return {
        "reporters": reporters_df.to_dict("records"),
        "partners": partners_df.to_dict("records"),
        "commodities": commodities_df.to_dict("records"),
        "chokepoints": chokepoints_df.to_dict("records"),
        "periods": sorted(all_periods),
    }


def get_energy_indicator_options() -> list[dict[str, str]]:
    if not table_exists(ENERGY_TABLE):
        return []
    df = run_query(
        f"""
        select distinct indicator_code as value, indicator_name as label
        from {ENERGY_TABLE}
        where reporter_iso3 in (select reporter_iso3 from {_dataset_reporter_subquery()})
        order by indicator_name
        """
    )
    return df.to_dict("records")


def get_event_options() -> list[dict[str, str]]:
    if not table_exists(EVENT_DIM_TABLE):
        return []
    df = run_query(
        f"""
        select
            d.event_id as value,
            d.event_name || ' [' || d.severity_level || ', ' || strftime(d.event_start_date, '%Y-%m-%d') || ']' as label
        from {EVENT_DIM_TABLE} d
        order by d.event_start_date desc, d.event_name
        """
    )
    return df.to_dict("records")


def get_default_event_id() -> str | None:
    if not table_exists(EVENT_DIM_TABLE) or not table_exists(TRADE_SUMMARY_TABLE):
        return None
    return run_scalar(
        f"""
        select d.event_id
        from {EVENT_DIM_TABLE} d
        left join {EVENT_IMPACT_TABLE} m
            on d.event_id = m.event_id
        where d.event_start_date <= (
            select max(month_start_date)
            from {TRADE_SUMMARY_TABLE}
        )
        order by coalesce(m.affected_chokepoint_count, 0) desc, d.event_start_date desc
        limit 1
        """
    )


def get_default_commodity_code(filters: dict[str, object], event_id: str | None = None) -> str | None:
    if filters.get("commodities"):
        return filters["commodities"][0]
    if not table_exists(FACT_TABLE):
        return None

    fact_where, fact_params = _fact_filter_clause(filters, include_commodities=False, include_period=event_id is None)
    event_join = ""
    params: list[object] = list(fact_params)
    if event_id:
        event_join = f"""
            inner join (
                select distinct year_month
                from {EVENT_MONTH_TABLE}
                where event_id = ?
            ) em
                on f.year_month = em.year_month
        """
        params.append(event_id)

    return run_scalar(
        f"""
        select f.cmd_code
        from {FACT_TABLE} f
        {event_join}
        {fact_where}
        group by f.cmd_code
        order by sum(f.trade_value_usd) desc
        limit 1
        """,
        params,
    )


def get_data_freshness() -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    for label, table_name in (
        ("Trade summary mart", TRADE_SUMMARY_TABLE),
        ("Trade dependence fact", FACT_TABLE),
        ("Trade exposure mart", TRADE_EXPOSURE_TABLE),
        ("PortWatch traffic", PORTWATCH_TABLE),
        ("Event month bridge", EVENT_MONTH_TABLE),
        ("Energy vulnerability mart", ENERGY_TABLE),
    ):
        if not table_exists(table_name):
            continue
        coverage_df = run_query(
            f"""
            select min(year_month) as coverage_start, max(year_month) as coverage_end
            from {table_name}
            """
        )
        rows.append(
            {
                "dataset_name": label,
                "coverage_start": coverage_df.at[0, "coverage_start"],
                "coverage_end": coverage_df.at[0, "coverage_end"],
            }
        )
    return pd.DataFrame(rows)


def _overview_uses_fact_scope(filters: dict[str, object]) -> bool:
    return bool(filters.get("partners")) or bool(filters.get("commodities"))


def get_overview_trade_trend(filters: dict[str, object]) -> pd.DataFrame:
    if _overview_uses_fact_scope(filters):
        fact_where, fact_params = _fact_filter_clause(filters)
        return run_query(
            f"""
            select
                {_month_start_expr('f.year_month')} as month_start_date,
                f.year_month,
                sum(f.trade_value_usd) as trade_value_usd
            from {FACT_TABLE} f
            {fact_where}
            group by 1, 2
            order by 1
            """,
            fact_params,
        )

    measure_column = _trade_value_column(filters)
    summary_where, summary_params = _summary_filter_clause(filters, reporter_column="reporter_iso3")
    return run_query(
        f"""
        select
            month_start_date,
            year_month,
            sum({measure_column}) as trade_value_usd
        from {TRADE_SUMMARY_TABLE}
        {summary_where}
        group by 1, 2
        order by 1
        """,
        summary_params,
    )


def get_overview_top_reporters(filters: dict[str, object], latest_month: str) -> pd.DataFrame:
    if not latest_month:
        return pd.DataFrame()

    if _overview_uses_fact_scope(filters):
        scoped_filters = dict(filters)
        scoped_filters["start_period"] = latest_month
        scoped_filters["end_period"] = latest_month
        fact_where, fact_params = _fact_filter_clause(scoped_filters)
        return run_query(
            f"""
            select
                coalesce(r.reporter_country_name, f.reporter_iso3) as reporter_country_name,
                f.reporter_iso3,
                sum(f.trade_value_usd) as trade_value_usd,
                ? as trade_flow_scope
            from {FACT_TABLE} f
            left join {_reporter_name_subquery()} r
                on f.reporter_iso3 = r.reporter_iso3
            {fact_where}
            group by 1, 2
            order by trade_value_usd desc
            limit ?
            """,
            [get_trade_flow_label(filters), *fact_params, int(filters["top_n"])],
        )

    measure_column = _trade_value_column(filters)
    scoped_filters = dict(filters)
    scoped_filters["start_period"] = latest_month
    scoped_filters["end_period"] = latest_month
    summary_where, summary_params = _summary_filter_clause(scoped_filters, reporter_column="reporter_iso3")
    return run_query(
        f"""
        select
            {_clean_country_name_expr('reporter_country_name')} as reporter_country_name,
            reporter_iso3,
            sum({measure_column}) as trade_value_usd,
            ? as trade_flow_scope
        from {TRADE_SUMMARY_TABLE}
        {summary_where}
        group by 1, 2
        order by trade_value_usd desc
        limit ?
        """,
        [get_trade_flow_label(filters), *summary_params, int(filters["top_n"])],
    )


def get_overview_latest_exposure_month(filters: dict[str, object]) -> str | None:
    where_sql, params = _chokepoint_filter_clause(filters, chokepoint_column="chokepoint_name")
    reporter_clause, reporter_params = _summary_filter_clause(filters, reporter_column="reporter_iso3", period_column=None)
    combined_where = [where_sql.replace("where ", "")] if where_sql else []
    if reporter_clause:
        combined_where.append(reporter_clause.replace("where ", ""))
    sql = f"""
        select max(year_month)
        from {CHOKEPOINT_EXPOSURE_TABLE}
        {"where " + " and ".join(filter(None, combined_where)) if combined_where else ""}
    """
    return run_scalar(sql, [*params, *reporter_params])


def get_overview_latest_exposure(filters: dict[str, object], latest_month: str) -> pd.DataFrame:
    if not latest_month:
        return pd.DataFrame()

    sql = f"""
        select
            {_clean_country_name_expr('reporter_country_name')} as reporter_country_name,
            reporter_iso3,
            chokepoint_name,
            chokepoint_trade_exposure_ratio,
            chokepoint_trade_value_usd,
            stress_index_weighted,
            active_event_count
        from {CHOKEPOINT_EXPOSURE_TABLE}
        where reporter_iso3 not in ('EUR', 'W00')
          and year_month = ?
    """
    params: list[object] = [latest_month]
    if filters.get("reporters"):
        clause, clause_params = _build_in_clause("reporter_iso3", filters["reporters"])
        sql += f" and {clause}"
        params.extend(clause_params)
    if filters.get("chokepoints"):
        clause, clause_params = _build_in_clause("chokepoint_name", filters["chokepoints"])
        sql += f" and {clause}"
        params.extend(clause_params)
    sql += " order by chokepoint_trade_exposure_ratio desc nulls last limit ?"
    params.append(int(filters["top_n"]))
    return run_query(sql, params)


def get_trade_corridors(filters: dict[str, object]) -> pd.DataFrame:
    fact_where, fact_params = _fact_filter_clause(filters)
    return run_query(
        f"""
        select
            coalesce(r.reporter_country_name, f.reporter_iso3) as reporter_country_name,
            f.reporter_iso3,
            {_clean_country_name_expr('coalesce(c.country_name, f.partner_iso3)')} as partner_country_name,
            f.partner_iso3,
            f.trade_flow,
            sum(f.trade_value_usd) as trade_value_usd,
            count(distinct f.cmd_code) as commodity_count
        from {FACT_TABLE} f
        left join {_reporter_name_subquery()} r
            on f.reporter_iso3 = r.reporter_iso3
        left join {COUNTRY_DIM_TABLE} c
            on f.partner_iso3 = c.iso3
        {fact_where}
        group by 1, 2, 3, 4, 5
        order by trade_value_usd desc
        limit ?
        """,
        [*fact_params, int(filters["top_n"])],
    )


def get_trade_commodity_dependence(filters: dict[str, object]) -> pd.DataFrame:
    fact_where, fact_params = _fact_filter_clause(filters, include_commodities=True)
    return run_query(
        f"""
        select
            {_commodity_label_expr('d.commodity_name', 'f.cmd_code')} as commodity_label,
            coalesce(d.commodity_group, 'Unclassified') as commodity_group,
            f.trade_flow,
            sum(f.trade_value_usd) as trade_value_usd,
            count(distinct f.partner_iso3) as partner_count
        from {FACT_TABLE} f
        left join {COMMODITY_DIM_TABLE} d
            on f.cmd_code = d.cmd_code
        {fact_where}
        group by 1, 2, 3
        order by trade_value_usd desc
        limit ?
        """,
        [*fact_params, int(filters["top_n"])],
    )


def get_partner_concentration_table(filters: dict[str, object]) -> pd.DataFrame:
    fact_where, fact_params = _fact_filter_clause(filters)
    return run_query(
        f"""
        with corridors as (
            select
                coalesce(r.reporter_country_name, f.reporter_iso3) as reporter_country_name,
                f.reporter_iso3,
                {_clean_country_name_expr('coalesce(c.country_name, f.partner_iso3)')} as partner_country_name,
                f.partner_iso3,
                f.trade_flow,
                sum(f.trade_value_usd) as trade_value_usd
            from {FACT_TABLE} f
            left join {_reporter_name_subquery()} r
                on f.reporter_iso3 = r.reporter_iso3
            left join {COUNTRY_DIM_TABLE} c
                on f.partner_iso3 = c.iso3
            {fact_where}
            group by 1, 2, 3, 4, 5
        ),
        reporter_totals as (
            select
                reporter_iso3,
                trade_flow,
                sum(trade_value_usd) as reporter_trade_value_usd
            from corridors
            group by 1, 2
        )
        select
            corridors.reporter_country_name,
            corridors.partner_country_name,
            corridors.trade_flow,
            corridors.trade_value_usd,
            reporter_totals.reporter_trade_value_usd,
            corridors.trade_value_usd / nullif(reporter_totals.reporter_trade_value_usd, 0) as reporter_trade_share
        from corridors
        left join reporter_totals
            on corridors.reporter_iso3 = reporter_totals.reporter_iso3
           and corridors.trade_flow = reporter_totals.trade_flow
        order by reporter_trade_share desc nulls last, corridors.trade_value_usd desc
        limit ?
        """,
        [*fact_params, int(DISPLAY_TABLE_LIMIT)],
    )


def get_latest_stress_month(filters: dict[str, object]) -> str | None:
    where_sql, params = _chokepoint_filter_clause(
        filters,
        chokepoint_column=_portwatch_canonical_expr("chokepoint_name"),
    )
    return run_scalar(f"select max(year_month) from {PORTWATCH_TABLE} {where_sql}", params)


def get_chokepoint_stress_trend(filters: dict[str, object]) -> pd.DataFrame:
    normalized_name = _portwatch_canonical_expr("p.chokepoint_name")
    where_sql, params = _chokepoint_filter_clause(
        filters,
        chokepoint_column=normalized_name,
    )
    return run_query(
        f"""
        select
            {_month_start_expr('p.year_month')} as month_start_date,
            p.year_month,
            {normalized_name} as chokepoint_name,
            p.avg_n_total,
            p.avg_capacity,
            p.stress_index,
            p.stress_index_weighted
        from {PORTWATCH_TABLE} p
        {where_sql}
        order by month_start_date, chokepoint_name
        """,
        params,
    )


def get_latest_stress_ranking(filters: dict[str, object], latest_month: str) -> pd.DataFrame:
    if not latest_month:
        return pd.DataFrame()
    normalized_name = _portwatch_canonical_expr("p.chokepoint_name")
    sql = f"""
        select
            {normalized_name} as chokepoint_name,
            p.avg_n_total,
            p.avg_capacity,
            p.stress_index,
            p.stress_index_weighted
        from {PORTWATCH_TABLE} p
        where p.year_month = ?
    """
    params: list[object] = [latest_month]
    if filters.get("chokepoints"):
        clause, clause_params = _build_in_clause(normalized_name, filters["chokepoints"])
        sql += f" and {clause}"
        params.extend(clause_params)
    sql += " order by p.stress_index_weighted desc nulls last, p.avg_n_total desc nulls last limit ?"
    params.append(int(filters["top_n"]))
    return run_query(sql, params)


def get_latest_exposure_month(filters: dict[str, object]) -> str | None:
    sql = f"""
        select max(year_month)
        from {CHOKEPOINT_EXPOSURE_TABLE}
        where reporter_iso3 not in ('EUR', 'W00')
    """
    params: list[object] = []
    if filters.get("reporters"):
        clause, clause_params = _build_in_clause("reporter_iso3", filters["reporters"])
        sql += f" and {clause}"
        params.extend(clause_params)
    if filters.get("chokepoints"):
        clause, clause_params = _build_in_clause("chokepoint_name", filters["chokepoints"])
        sql += f" and {clause}"
        params.extend(clause_params)
    if filters.get("start_period") and filters.get("end_period"):
        sql += " and year_month between ? and ?"
        params.extend([filters["start_period"], filters["end_period"]])
    return run_scalar(sql, params)


def get_chokepoint_exposure_scatter(filters: dict[str, object], latest_month: str) -> pd.DataFrame:
    if not latest_month:
        return pd.DataFrame()
    sql = f"""
        select
            {_clean_country_name_expr('reporter_country_name')} as reporter_country_name,
            reporter_iso3,
            chokepoint_name,
            stress_index_weighted,
            chokepoint_trade_exposure_ratio,
            chokepoint_trade_value_usd,
            active_event_count
        from {CHOKEPOINT_EXPOSURE_TABLE}
        where year_month = ?
          and reporter_iso3 not in ('EUR', 'W00')
          and stress_index_weighted is not null
          and chokepoint_trade_exposure_ratio is not null
    """
    params: list[object] = [latest_month]
    if filters.get("reporters"):
        clause, clause_params = _build_in_clause("reporter_iso3", filters["reporters"])
        sql += f" and {clause}"
        params.extend(clause_params)
    if filters.get("chokepoints"):
        clause, clause_params = _build_in_clause("chokepoint_name", filters["chokepoints"])
        sql += f" and {clause}"
        params.extend(clause_params)
    sql += " order by chokepoint_trade_exposure_ratio desc nulls last limit ?"
    params.append(max(int(filters["top_n"]) * 15, 100))
    return run_query(sql, params)


def get_chokepoint_commodity_table(filters: dict[str, object]) -> pd.DataFrame:
    if not table_exists(ROUTE_FACT_TABLE):
        return pd.DataFrame()

    fact_where, fact_params = _fact_filter_clause(
        filters,
        reporter_column="rf.reporter_iso3",
        partner_column="rf.partner_iso3",
        commodity_column="rf.cmd_code",
        period_column="rf.year_month",
        flow_column="rf.trade_flow",
    )

    sql = f"""
        select
            coalesce(r.reporter_country_name, rf.reporter_iso3) as reporter_country_name,
            rf.main_chokepoint as chokepoint_name,
            {_commodity_label_expr('d.commodity_name', 'rf.cmd_code')} as commodity_label,
            rf.trade_flow,
            sum(rf.trade_value_usd) as trade_value_usd
        from {ROUTE_FACT_TABLE} rf
        left join {_reporter_name_subquery()} r
            on rf.reporter_iso3 = r.reporter_iso3
        left join {COMMODITY_DIM_TABLE} d
            on rf.cmd_code = d.cmd_code
        {fact_where}
        {"and" if fact_where else "where"} rf.main_chokepoint is not null
        """
    params = list(fact_params)
    if filters.get("chokepoints"):
        clause, clause_params = _build_in_clause("rf.main_chokepoint", filters["chokepoints"])
        sql += f" and {clause}"
        params.extend(clause_params)
    sql += """
        group by 1, 2, 3, 4
        order by trade_value_usd desc
        limit ?
    """
    params.append(int(DISPLAY_TABLE_LIMIT))
    return run_query(sql, params)


def get_event_summary(event_id: str) -> pd.DataFrame:
    if not table_exists(EVENT_DIM_TABLE):
        return pd.DataFrame()
    return run_query(
        f"""
        select
            d.event_id,
            d.event_name,
            d.event_type,
            d.event_start_date,
            d.event_end_date,
            d.severity_level,
            d.description,
            m.affected_country_count,
            m.affected_chokepoint_count,
            m.mean_throughput_pct_change_vs_baseline,
            m.mean_event_window_zscore_capacity,
            m.mean_event_window_zscore_count
        from {EVENT_DIM_TABLE} d
        left join {EVENT_IMPACT_TABLE} m
            on d.event_id = m.event_id
        where d.event_id = ?
        """,
        [event_id],
    )


def get_event_month_window(event_id: str) -> pd.DataFrame:
    if not table_exists(EVENT_MONTH_TABLE):
        return pd.DataFrame()
    return run_query(
        f"""
        select
            year_month,
            month_start_date,
            has_active_phase,
            is_event_active,
            is_lead_period,
            is_lag_period,
            case
                when has_active_phase or is_event_active then 'Active'
                when is_lead_period then 'Lead'
                when is_lag_period then 'Lag'
                else 'Context'
            end as phase_label
        from {EVENT_MONTH_TABLE}
        where event_id = ?
        order by month_start_date
        """,
        [event_id],
    )


def get_event_trade_window(event_id: str, filters: dict[str, object], commodity_code: str) -> pd.DataFrame:
    if not commodity_code or not table_exists(FACT_TABLE) or not table_exists(EVENT_MONTH_TABLE):
        return pd.DataFrame()
    fact_where, fact_params = _fact_filter_clause(filters, include_commodities=False, include_period=False)
    return run_query(
        f"""
        with event_months as (
            select
                year_month,
                month_start_date,
                case
                    when has_active_phase or is_event_active then 'Active'
                    when is_lead_period then 'Lead'
                    when is_lag_period then 'Lag'
                    else 'Context'
                end as phase_label
            from {EVENT_MONTH_TABLE}
            where event_id = ?
        ),
        scoped_trade as (
            select
                f.year_month,
                sum(f.trade_value_usd) as trade_value_usd
            from {FACT_TABLE} f
            {fact_where}
              and f.cmd_code = ?
            group by 1
        )
        select
            em.month_start_date,
            em.year_month,
            em.phase_label,
            coalesce(st.trade_value_usd, 0) as trade_value_usd
        from event_months em
        left join scoped_trade st
            on em.year_month = st.year_month
        order by em.month_start_date
        """,
        [event_id, *fact_params, commodity_code],
    )


def get_event_chokepoint_window(event_id: str, filters: dict[str, object]) -> pd.DataFrame:
    if not table_exists(PORTWATCH_TABLE) or not table_exists(EVENT_MONTH_TABLE) or not table_exists(EVENT_CHOKEPOINT_TABLE):
        return pd.DataFrame()
    normalized_portwatch = _portwatch_canonical_expr("p.chokepoint_name")
    normalized_event = _event_canonical_expr("ec.chokepoint_name")
    sql = f"""
        with event_months as (
            select
                year_month,
                month_start_date,
                case
                    when has_active_phase or is_event_active then 'Active'
                    when is_lead_period then 'Lead'
                    when is_lag_period then 'Lag'
                    else 'Context'
                end as phase_label
            from {EVENT_MONTH_TABLE}
            where event_id = ?
        ),
        affected_chokepoints as (
            select distinct {normalized_event} as chokepoint_name
            from {EVENT_CHOKEPOINT_TABLE} ec
            where ec.event_id = ?
        )
        select
            em.month_start_date,
            em.year_month,
            em.phase_label,
            {normalized_portwatch} as chokepoint_name,
            p.avg_n_total,
            p.avg_capacity,
            p.stress_index_weighted
        from event_months em
        left join {PORTWATCH_TABLE} p
            on em.year_month = p.year_month
        where (
            {normalized_portwatch} in (select chokepoint_name from affected_chokepoints)
            or not exists (select 1 from affected_chokepoints)
        )
    """
    params: list[object] = [event_id, event_id]
    if filters.get("chokepoints"):
        clause, clause_params = _build_in_clause(normalized_portwatch, filters["chokepoints"])
        sql += f" and {clause}"
        params.extend(clause_params)
    sql += " order by em.month_start_date, chokepoint_name"
    return run_query(sql, params)


def get_commodity_trend(filters: dict[str, object], commodity_code: str) -> pd.DataFrame:
    if not commodity_code or not table_exists(FACT_TABLE):
        return pd.DataFrame()
    fact_where, fact_params = _fact_filter_clause(filters, include_commodities=False)
    return run_query(
        f"""
        select
            {_month_start_expr('f.year_month')} as month_start_date,
            f.year_month,
            sum(f.trade_value_usd) as trade_value_usd
        from {FACT_TABLE} f
        {fact_where}
          and f.cmd_code = ?
        group by 1, 2
        order by 1
        """,
        [*fact_params, commodity_code],
    )


def get_event_before_after_table(
    event_trade_window_df: pd.DataFrame,
    event_chokepoint_window_df: pd.DataFrame,
) -> pd.DataFrame:
    rows: list[dict[str, object]] = []

    if not event_trade_window_df.empty:
        trade_phase = event_trade_window_df.groupby("phase_label", as_index=False)["trade_value_usd"].mean()
        trade_lookup = dict(zip(trade_phase["phase_label"], trade_phase["trade_value_usd"]))
        lead_value = trade_lookup.get("Lead")
        active_value = trade_lookup.get("Active")
        lag_value = trade_lookup.get("Lag")
        rows.append(
            {
                "metric": "Selected commodity trade value",
                "lead_average": lead_value,
                "active_average": active_value,
                "lag_average": lag_value,
                "active_vs_lead_pct_change": None
                if lead_value in (None, 0) or pd.isna(lead_value) or active_value is None
                else (active_value - lead_value) / lead_value,
            }
        )

    if not event_chokepoint_window_df.empty:
        traffic_phase = event_chokepoint_window_df.groupby("phase_label", as_index=False)["avg_n_total"].mean()
        traffic_lookup = dict(zip(traffic_phase["phase_label"], traffic_phase["avg_n_total"]))
        lead_value = traffic_lookup.get("Lead")
        active_value = traffic_lookup.get("Active")
        lag_value = traffic_lookup.get("Lag")
        rows.append(
            {
                "metric": "Affected chokepoint traffic",
                "lead_average": lead_value,
                "active_average": active_value,
                "lag_average": lag_value,
                "active_vs_lead_pct_change": None
                if lead_value in (None, 0) or pd.isna(lead_value) or active_value is None
                else (active_value - lead_value) / lead_value,
            }
        )

    return pd.DataFrame(rows)


def get_energy_latest_year(filters: dict[str, object], indicator_code: str) -> int | None:
    if not table_exists(ENERGY_TABLE):
        return None
    sql = f"""
        select max(year)
        from {ENERGY_TABLE}
        where reporter_iso3 in (select reporter_iso3 from {_dataset_reporter_subquery()})
          and indicator_code = ?
          and {_energy_non_null_condition()}
    """
    params: list[object] = [indicator_code]
    if filters.get("reporters"):
        clause, clause_params = _build_in_clause("reporter_iso3", filters["reporters"])
        sql += f" and {clause}"
        params.extend(clause_params)
    return run_scalar(sql, params)


def get_energy_latest_ranking(filters: dict[str, object], indicator_code: str, latest_year: int | None) -> pd.DataFrame:
    if latest_year is None:
        return pd.DataFrame()
    sql = f"""
        select
            {_clean_country_name_expr('reporter_country_name')} as reporter_country_name,
            reporter_iso3,
            indicator_name,
            indicator_value,
            unit,
            year
        from {ENERGY_TABLE}
        where reporter_iso3 in (select reporter_iso3 from {_dataset_reporter_subquery()})
          and indicator_code = ?
          and year = ?
          and {_energy_non_null_condition()}
    """
    params: list[object] = [indicator_code, latest_year]
    if filters.get("reporters"):
        clause, clause_params = _build_in_clause("reporter_iso3", filters["reporters"])
        sql += f" and {clause}"
        params.extend(clause_params)
    sql += " order by indicator_value desc nulls last limit ?"
    params.append(int(filters["top_n"]))
    return run_query(sql, params)


def get_energy_trend(
    filters: dict[str, object],
    indicator_code: str,
    reporter_ids: Sequence[str],
) -> pd.DataFrame:
    if not reporter_ids:
        return pd.DataFrame()
    clause, reporter_params = _build_in_clause("reporter_iso3", reporter_ids)
    sql = f"""
        select
            year,
            year_month,
            month_start_date,
            {_clean_country_name_expr('reporter_country_name')} as reporter_country_name,
            reporter_iso3,
            indicator_value
        from {ENERGY_TABLE}
        where reporter_iso3 in (select reporter_iso3 from {_dataset_reporter_subquery()})
          and indicator_code = ?
          and {_energy_non_null_condition()}
          and {clause}
        order by month_start_date, reporter_country_name
    """
    return run_query(sql, [indicator_code, *reporter_params])


def get_energy_context_table(filters: dict[str, object], indicator_code: str, latest_year: int | None) -> pd.DataFrame:
    if latest_year is None or not table_exists(ENERGY_TABLE):
        return pd.DataFrame()

    measure_column = _trade_value_column(filters)
    if table_exists(TRADE_EXPOSURE_TABLE):
        latest_exposure_cte = f"""
        latest_exposure_rows as (
            select
                reporter_iso3,
                year_month,
                chokepoint_trade_exposure_ratio,
                row_number() over (
                    partition by reporter_iso3
                    order by year_month desc
                ) as reporter_month_rank
            from {TRADE_EXPOSURE_TABLE}
            where reporter_iso3 not in ('EUR', 'W00')
        ),
        latest_exposure as (
            select
                reporter_iso3,
                year_month as latest_exposure_month,
                max(chokepoint_trade_exposure_ratio) as peak_exposure_ratio
            from latest_exposure_rows
            where reporter_month_rank = 1
            group by 1, 2
        ),
        """
    else:
        latest_exposure_cte = """
        latest_exposure as (
            select
                cast(null as varchar) as reporter_iso3,
                cast(null as varchar) as latest_exposure_month,
                cast(null as double) as peak_exposure_ratio
            where false
        ),
        """

    if table_exists(TRADE_COMMODITY_TABLE):
        energy_trade_cte = f"""
        energy_trade as (
            select
                reporter_iso3,
                sum({measure_column}) as energy_trade_value_usd
            from {TRADE_COMMODITY_TABLE}
            where reporter_iso3 not in ('EUR', 'W00')
              and cmd_code in ('2709', '2710')
              and year = ?
            group by 1
        )
        """
        params: list[object] = [indicator_code, latest_year, latest_year]
    else:
        energy_trade_cte = """
        energy_trade as (
            select
                cast(null as varchar) as reporter_iso3,
                cast(null as double) as energy_trade_value_usd
            where false
        )
        """
        params = [indicator_code, latest_year]

    sql = f"""
        with energy_ranking as (
            select
                {_clean_country_name_expr('reporter_country_name')} as reporter_country_name,
                reporter_iso3,
                indicator_name,
                indicator_value,
                unit
            from {ENERGY_TABLE}
            where reporter_iso3 in (select reporter_iso3 from {_dataset_reporter_subquery()})
              and indicator_code = ?
              and year = ?
              and {_energy_non_null_condition()}
        ),
        {latest_exposure_cte}
        {energy_trade_cte}
        select
            energy_ranking.reporter_country_name,
            energy_ranking.reporter_iso3,
            energy_ranking.indicator_name,
            energy_ranking.indicator_value,
            energy_ranking.unit,
            energy_trade.energy_trade_value_usd,
            latest_exposure.latest_exposure_month,
            latest_exposure.peak_exposure_ratio
        from energy_ranking
        left join energy_trade
            on energy_ranking.reporter_iso3 = energy_trade.reporter_iso3
        left join latest_exposure
            on energy_ranking.reporter_iso3 = latest_exposure.reporter_iso3
    """
    if filters.get("reporters"):
        clause, clause_params = _build_in_clause("energy_ranking.reporter_iso3", filters["reporters"])
        sql += f" where {clause}"
        params.extend(clause_params)
    sql += " order by indicator_value desc nulls last limit ?"
    params.append(int(DISPLAY_TABLE_LIMIT))
    return run_query(sql, params)
