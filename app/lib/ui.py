from __future__ import annotations

from typing import Iterable

import altair as alt
import pandas as pd
import streamlit as st

from app.lib.config import APP_SUBTITLE, APP_TITLE, DEFAULT_CHART_HEIGHT


def configure_page() -> None:
    st.set_page_config(
        page_title=APP_TITLE,
        page_icon="",
        layout="wide",
        initial_sidebar_state="expanded",
    )


def apply_base_style() -> None:
    st.markdown(
        """
        <style>
            .block-container {
                padding-top: 1.6rem;
                padding-bottom: 2rem;
            }
            div[data-testid="stMetric"] {
                background: #f7f8fa;
                border: 1px solid #e6e9ef;
                border-radius: 14px;
                padding: 0.85rem 1rem;
            }
            div[data-testid="stDataFrame"] {
                border: 1px solid #e6e9ef;
                border-radius: 12px;
            }
            .dashboard-subtitle {
                color: #4b5563;
                font-size: 0.98rem;
                margin-top: -0.3rem;
                margin-bottom: 1.1rem;
            }
            .section-note {
                color: #6b7280;
                font-size: 0.9rem;
            }
        </style>
        """,
        unsafe_allow_html=True,
    )


def render_app_shell() -> None:
    st.sidebar.caption(APP_SUBTITLE)


def render_page_header(title: str, subtitle: str) -> None:
    st.title(title)
    st.markdown(f"<div class='dashboard-subtitle'>{subtitle}</div>", unsafe_allow_html=True)


def render_missing_table(message: str) -> None:
    st.warning(message)


def render_empty_state(message: str) -> None:
    st.info(message)


def render_methodology_expander(lines: list[str]) -> None:
    with st.expander("Methodology"):
        for line in lines:
            st.markdown(f"- {line}")


def format_usd(value: float | int | None) -> str:
    if value is None or pd.isna(value):
        return "n/a"
    abs_value = abs(float(value))
    if abs_value >= 1_000_000_000:
        return f"${value / 1_000_000_000:.1f}B"
    if abs_value >= 1_000_000:
        return f"${value / 1_000_000:.1f}M"
    if abs_value >= 1_000:
        return f"${value / 1_000:.1f}K"
    return f"${value:,.0f}"


def format_pct(value: float | int | None) -> str:
    if value is None or pd.isna(value):
        return "n/a"
    return f"{value:.1%}"


def format_number(value: float | int | None, digits: int = 0) -> str:
    if value is None or pd.isna(value):
        return "n/a"
    if digits == 0:
        return f"{value:,.0f}"
    return f"{value:,.{digits}f}"


def render_line_chart(
    df: pd.DataFrame,
    x: str,
    y: str,
    color: str | None = None,
    tooltip: Iterable[str] | None = None,
    height: int = DEFAULT_CHART_HEIGHT,
) -> None:
    if df.empty:
        render_empty_state("No data available for this chart under the current filters.")
        return

    encoding = {
        "x": alt.X(x, title=None),
        "y": alt.Y(y, title=None),
        "tooltip": list(tooltip or df.columns),
    }
    if color:
        encoding["color"] = alt.Color(color, title=None)

    chart = (
        alt.Chart(df)
        .mark_line(point=True, strokeWidth=2)
        .encode(**encoding)
        .properties(height=height)
    )
    st.altair_chart(chart, use_container_width=True)


def render_bar_chart(
    df: pd.DataFrame,
    x: str,
    y: str,
    color: str | None = None,
    tooltip: Iterable[str] | None = None,
    height: int = DEFAULT_CHART_HEIGHT,
) -> None:
    if df.empty:
        render_empty_state("No data available for this chart under the current filters.")
        return

    encoding = {
        "x": alt.X(x, title=None),
        "y": alt.Y(y, sort="-x", title=None),
        "tooltip": list(tooltip or df.columns),
    }
    if color:
        encoding["color"] = alt.Color(color, title=None, legend=None)

    chart = (
        alt.Chart(df)
        .mark_bar(cornerRadiusTopRight=4, cornerRadiusBottomRight=4)
        .encode(**encoding)
        .properties(height=height)
    )
    st.altair_chart(chart, use_container_width=True)


def render_scatter_chart(
    df: pd.DataFrame,
    x: str,
    y: str,
    color: str | None = None,
    size: str | None = None,
    tooltip: Iterable[str] | None = None,
    height: int = DEFAULT_CHART_HEIGHT,
) -> None:
    if df.empty:
        render_empty_state("No data available for this chart under the current filters.")
        return

    encoding = {
        "x": alt.X(x, title=None),
        "y": alt.Y(y, title=None),
        "tooltip": list(tooltip or df.columns),
    }
    if color:
        encoding["color"] = alt.Color(color, title=None)
    if size:
        encoding["size"] = alt.Size(size, title=None)

    chart = alt.Chart(df).mark_circle(opacity=0.75).encode(**encoding).properties(height=height)
    st.altair_chart(chart, use_container_width=True)
