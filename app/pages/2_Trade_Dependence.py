from __future__ import annotations

import sys
from pathlib import Path

import altair as alt
import pandas as pd
import streamlit as st

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app.lib.config import ACCENT_COLOR, MUTED_COLOR
from app.lib.db import table_exists
from app.lib.export import render_csv_download
from app.lib.filters import render_sidebar
from app.lib.narrative import dependence_interpretation, dependence_subtitle, scope_caption
from app.lib.queries import FACT_TABLE, get_partner_concentration_table, get_trade_commodity_dependence, get_trade_corridors
from app.lib.ui import (
    format_pct,
    format_usd,
    render_empty_state,
    render_methodology_expander,
    render_missing_table,
    render_page_header,
)


filters = render_sidebar()
render_page_header("Trade Dependence", dependence_subtitle())
st.caption(scope_caption(filters))

if not table_exists(FACT_TABLE):
    render_missing_table("`analytics_marts.fct_reporter_partner_commodity_month` is required for the trade dependence page.")
    st.stop()

corridors_df = get_trade_corridors(filters)
commodity_df = get_trade_commodity_dependence(filters)
concentration_df = get_partner_concentration_table(filters)

if corridors_df.empty:
    render_empty_state("No bilateral trade corridors matched the current filters.")
    st.stop()

top_corridor = corridors_df.iloc[0]
largest_corridor_label = f"{top_corridor['reporter_country_name']} -> {top_corridor['partner_country_name']}"
highest_share = float(concentration_df.iloc[0]["reporter_trade_share"]) if not concentration_df.empty else None

col1, col2, col3, col4 = st.columns(4)
col1.metric("Largest corridor", format_usd(float(top_corridor["trade_value_usd"])))
col2.metric("Leading pair", largest_corridor_label)
col3.metric("Commodities in leading pair", f"{int(top_corridor['commodity_count'])}")
col4.metric("Highest partner share", format_pct(highest_share))

st.markdown(dependence_interpretation(corridors_df))

corridor_chart_df = corridors_df.copy()
corridor_chart_df["corridor_label"] = (
    corridor_chart_df["reporter_country_name"]
    + " -> "
    + corridor_chart_df["partner_country_name"]
    + " ("
    + corridor_chart_df["trade_flow"]
    + ")"
)
st.subheader("A small number of bilateral corridors dominate observed trade value")
corridor_chart = (
    alt.Chart(corridor_chart_df)
    .mark_bar(color=ACCENT_COLOR, cornerRadiusTopRight=4, cornerRadiusBottomRight=4)
    .encode(
        x=alt.X("trade_value_usd:Q", title="Trade value (USD)", axis=alt.Axis(format="$,.2s", grid=False)),
        y=alt.Y("corridor_label:N", sort="-x", title=None, axis=alt.Axis(labelLimit=260)),
        tooltip=[
            "reporter_country_name",
            "partner_country_name",
            "trade_flow",
            "commodity_count",
            "trade_value_usd",
        ],
    )
    .properties(height=380)
)
st.altair_chart(corridor_chart, use_container_width=True)
render_csv_download(corridors_df, "Download corridor ranking", "trade_dependence_corridors")

if commodity_df.empty:
    render_empty_state("No commodity dependence rows matched the current filters.")
else:
    commodity_chart_df = commodity_df.copy()
    commodity_chart_df["commodity_flow_label"] = (
        commodity_chart_df["commodity_label"] + " (" + commodity_chart_df["trade_flow"] + ")"
    )
    commodity_leader = commodity_df.iloc[0]["commodity_label"]
    st.subheader(f"{commodity_leader} is the largest commodity channel in the current scope")
    commodity_chart = (
        alt.Chart(commodity_chart_df)
        .mark_bar(color=MUTED_COLOR, cornerRadiusTopRight=4, cornerRadiusBottomRight=4)
        .encode(
            x=alt.X("trade_value_usd:Q", title="Trade value (USD)", axis=alt.Axis(format="$,.2s", grid=False)),
            y=alt.Y("commodity_flow_label:N", sort="-x", title=None, axis=alt.Axis(labelLimit=280)),
            tooltip=["commodity_label", "commodity_group", "trade_flow", "partner_count", "trade_value_usd"],
        )
        .properties(height=360)
    )
    st.altair_chart(commodity_chart, use_container_width=True)

st.subheader("Partner concentration shows where bilateral dependence becomes visible")
if concentration_df.empty:
    render_empty_state("No partner concentration table could be computed for the current scope.")
else:
    concentration_display = concentration_df.copy()
    concentration_display["trade_value_usd"] = concentration_display["trade_value_usd"].map(format_usd)
    concentration_display["reporter_trade_value_usd"] = concentration_display["reporter_trade_value_usd"].map(format_usd)
    concentration_display["reporter_trade_share"] = concentration_display["reporter_trade_share"].map(format_pct)
    st.dataframe(
        concentration_display.rename(
            columns={
                "reporter_country_name": "Reporter",
                "partner_country_name": "Partner",
                "trade_flow": "Flow",
                "trade_value_usd": "Corridor value",
                "reporter_trade_value_usd": "Reporter total",
                "reporter_trade_share": "Partner share",
            }
        ),
        use_container_width=True,
        hide_index=True,
    )
    render_csv_download(concentration_df, "Download partner concentration table", "trade_dependence_concentration")

render_methodology_expander(
    [
        "All three views on this page read from `analytics_marts.fct_reporter_partner_commodity_month` so the analysis stays at the canonical reporter-partner-commodity-month-flow grain.",
        "Partner concentration is computed as each reporter-partner corridor divided by the reporter total within the same selected flow.",
        "Commodity labels come from `analytics_staging.stg_dim_commodity` when present and always retain the commodity code in brackets.",
    ]
)
