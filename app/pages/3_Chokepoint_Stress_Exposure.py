from __future__ import annotations

import sys
from pathlib import Path

import altair as alt
import pandas as pd
import streamlit as st

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app.lib.config import ACCENT_COLOR, CAUTION_COLOR, MUTED_COLOR
from app.lib.db import table_exists
from app.lib.export import render_csv_download
from app.lib.filters import render_sidebar
from app.lib.narrative import chokepoint_interpretation, chokepoint_subtitle, scope_caption
from app.lib.queries import (
    CHOKEPOINT_EXPOSURE_TABLE,
    PORTWATCH_TABLE,
    ROUTE_FACT_TABLE,
    get_chokepoint_commodity_table,
    get_chokepoint_exposure_scatter,
    get_chokepoint_stress_trend,
    get_latest_exposure_month,
    get_latest_stress_month,
    get_latest_stress_ranking,
    get_overview_latest_exposure,
)
from app.lib.ui import (
    format_pct,
    format_usd,
    render_empty_state,
    render_methodology_expander,
    render_missing_table,
    render_page_header,
)


filters = render_sidebar()
render_page_header("Chokepoint Stress & Exposure", chokepoint_subtitle())
st.caption(scope_caption(filters))

has_portwatch = table_exists(PORTWATCH_TABLE)
has_exposure = table_exists(CHOKEPOINT_EXPOSURE_TABLE)

if not has_portwatch and not has_exposure:
    render_missing_table("This page needs PortWatch traffic data, reporter-level exposure data, or both. Neither table is available.")
    st.stop()

stress_trend_df = get_chokepoint_stress_trend(filters) if has_portwatch else pd.DataFrame()
latest_stress_month = get_latest_stress_month(filters) if has_portwatch else None
stress_ranking_df = get_latest_stress_ranking(filters, latest_stress_month) if latest_stress_month else pd.DataFrame()
latest_exposure_month = get_latest_exposure_month(filters) if has_exposure else None
scatter_df = get_chokepoint_exposure_scatter(filters, latest_exposure_month) if latest_exposure_month else pd.DataFrame()
route_df = get_chokepoint_commodity_table(filters) if table_exists(ROUTE_FACT_TABLE) else pd.DataFrame()

top_stressed = stress_ranking_df.iloc[0]["chokepoint_name"] if not stress_ranking_df.empty else "n/a"
top_exposed = scatter_df.iloc[0]["reporter_country_name"] if not scatter_df.empty else "n/a"
top_exposure_ratio = float(scatter_df.iloc[0]["chokepoint_trade_exposure_ratio"]) if not scatter_df.empty else None

col1, col2, col3, col4 = st.columns(4)
col1.metric("Latest traffic month", latest_stress_month or "n/a")
col2.metric("Latest exposure month", latest_exposure_month or "n/a")
col3.metric("Most stressed chokepoint", top_stressed)
col4.metric("Highest latest exposure", format_pct(top_exposure_ratio))

if not scatter_df.empty:
    st.markdown(chokepoint_interpretation(scatter_df))
elif not stress_ranking_df.empty:
    st.markdown(f"{top_stressed} carries the highest weighted traffic stress in the latest PortWatch snapshot.")

if stress_trend_df.empty and filters.get("chokepoints"):
    st.warning(
        "No PortWatch traffic rows matched the selected chokepoint filter. The exposure marts cover more chokepoints than the raw PortWatch traffic feed."
    )

if not stress_trend_df.empty:
    stress_title = (
        f"{top_stressed} carries the highest weighted stress in the latest traffic snapshot"
        if top_stressed != "n/a"
        else "Weighted chokepoint stress moves unevenly across the traffic network"
    )
    st.subheader(stress_title)
    stress_chart = (
        alt.Chart(stress_trend_df)
        .mark_line(point=True, strokeWidth=2.2)
        .encode(
            x=alt.X("month_start_date:T", title=None, axis=alt.Axis(grid=False)),
            y=alt.Y("stress_index_weighted:Q", title="Weighted stress", axis=alt.Axis(grid=False)),
            color=alt.Color("chokepoint_name:N", title=None),
            tooltip=["year_month", "chokepoint_name", "avg_n_total", "avg_capacity", "stress_index_weighted"],
        )
        .properties(height=360)
    )
    st.altair_chart(stress_chart, use_container_width=True)

support_col, ranking_col = st.columns((1.3, 1))
with support_col:
    if scatter_df.empty:
        render_empty_state("The latest exposure scatter is unavailable under the current filters.")
    else:
        st.subheader(f"{top_exposed} sits at the highest latest reporter-level exposure point")
        scatter_chart = (
            alt.Chart(scatter_df)
            .mark_circle(opacity=0.8, color=CAUTION_COLOR)
            .encode(
                x=alt.X("stress_index_weighted:Q", title="Weighted stress", axis=alt.Axis(grid=False)),
                y=alt.Y(
                    "chokepoint_trade_exposure_ratio:Q",
                    title="Trade exposure ratio",
                    axis=alt.Axis(format=".0%", grid=False),
                ),
                size=alt.Size("chokepoint_trade_value_usd:Q", title="Trade value"),
                tooltip=[
                    "reporter_country_name",
                    "reporter_iso3",
                    "chokepoint_name",
                    "chokepoint_trade_exposure_ratio",
                    "chokepoint_trade_value_usd",
                    "active_event_count",
                ],
            )
            .properties(height=340)
        )
        st.altair_chart(scatter_chart, use_container_width=True)
with ranking_col:
    if stress_ranking_df.empty:
        render_empty_state("No latest traffic ranking is available for the selected scope.")
    else:
        st.subheader("Latest traffic snapshot")
        ranking_display = stress_ranking_df.copy()
        ranking_display["avg_n_total"] = ranking_display["avg_n_total"].map(lambda value: f"{value:,.1f}")
        ranking_display["avg_capacity"] = ranking_display["avg_capacity"].map(lambda value: f"{value:,.0f}")
        ranking_display["stress_index_weighted"] = ranking_display["stress_index_weighted"].map(lambda value: f"{value:,.2f}")
        st.dataframe(
            ranking_display.rename(
                columns={
                    "chokepoint_name": "Chokepoint",
                    "avg_n_total": "Avg vessels",
                    "avg_capacity": "Avg capacity",
                    "stress_index": "Stress",
                    "stress_index_weighted": "Weighted stress",
                }
            ),
            use_container_width=True,
            hide_index=True,
        )

evidence_df = route_df if not route_df.empty else (
    get_overview_latest_exposure(filters, latest_exposure_month) if latest_exposure_month else pd.DataFrame()
)
if evidence_df.empty:
    render_empty_state("No bounded evidence table could be produced for the current filters.")
else:
    if not route_df.empty:
        st.subheader("Route-level commodity evidence shows where chokepoint dependence is actually carried")
        evidence_display = route_df.copy()
        evidence_display["trade_value_usd"] = evidence_display["trade_value_usd"].map(format_usd)
        st.dataframe(
            evidence_display.rename(
                columns={
                    "reporter_country_name": "Reporter",
                    "chokepoint_name": "Chokepoint",
                    "commodity_label": "Commodity",
                    "trade_flow": "Flow",
                    "trade_value_usd": "Trade value",
                }
            ),
            use_container_width=True,
            hide_index=True,
        )
        render_csv_download(route_df, "Download chokepoint commodity evidence", "chokepoint_route_evidence")
    else:
        st.subheader("Reporter-level exposure remains the best available evidence table in this warehouse")
        evidence_display = evidence_df.copy()
        evidence_display["chokepoint_trade_exposure_ratio"] = evidence_display["chokepoint_trade_exposure_ratio"].map(format_pct)
        evidence_display["chokepoint_trade_value_usd"] = evidence_display["chokepoint_trade_value_usd"].map(format_usd)
        st.dataframe(evidence_display, use_container_width=True, hide_index=True)
        render_csv_download(evidence_df, "Download latest exposure evidence", "chokepoint_latest_exposure")

render_methodology_expander(
    [
        "Traffic and weighted stress come from `analytics_staging.stg_portwatch_stress_metrics`, derived from the raw PortWatch monthly fact and aligned to the marts by canonicalized chokepoint names.",
        "Reporter exposure comes from `analytics_marts.mart_reporter_month_chokepoint_exposure` and is evaluated at the latest filtered exposure month.",
        "Route-level commodity evidence uses `analytics_marts.fct_reporter_partner_commodity_route_month` when that optional fact is present.",
        "The raw PortWatch feed covers fewer chokepoints than the exposure marts, so some chokepoints may appear in exposure views without a matching traffic trend.",
    ]
)
