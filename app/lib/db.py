from __future__ import annotations

from typing import Iterable, Sequence

import duckdb
import pandas as pd
import streamlit as st

from app.lib.config import resolve_db_path


@st.cache_resource(show_spinner=False)
def get_connection() -> duckdb.DuckDBPyConnection:
    db_path = resolve_db_path()
    return duckdb.connect(str(db_path), read_only=True)


@st.cache_data(show_spinner=False)
def run_query(sql: str, params: Sequence[object] = ()) -> pd.DataFrame:
    con = get_connection()
    return con.execute(sql, list(params)).df()


@st.cache_data(show_spinner=False)
def run_scalar(sql: str, params: Sequence[object] = ()) -> object | None:
    con = get_connection()
    row = con.execute(sql, list(params)).fetchone()
    if row is None:
        return None
    return row[0]


@st.cache_data(show_spinner=False)
def get_available_tables() -> set[str]:
    sql = """
        select table_schema || '.' || table_name as full_name
        from information_schema.tables
        where table_type = 'BASE TABLE'
        order by 1
    """
    df = run_query(sql)
    return set(df["full_name"].tolist())


def table_exists(full_name: str) -> bool:
    return full_name in get_available_tables()


@st.cache_data(show_spinner=False)
def get_table_columns(full_name: str) -> list[str]:
    schema_name, table_name = full_name.split(".", 1)
    sql = """
        select column_name
        from information_schema.columns
        where table_schema = ? and table_name = ?
        order by ordinal_position
    """
    df = run_query(sql, (schema_name, table_name))
    return df["column_name"].tolist()


def has_columns(full_name: str, required: Iterable[str]) -> bool:
    columns = set(get_table_columns(full_name))
    return all(column in columns for column in required)
