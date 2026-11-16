"""Read-only, aggregate-only Streamlit dashboard for CPD incident trends."""

from __future__ import annotations

from datetime import date, timedelta

import pandas as pd
import streamlit as st
from sqlalchemy import create_engine, text

from crime_pipeline.config import database_url


st.set_page_config(page_title="Local Crime Trends", page_icon="📊", layout="wide")
st.title("Cambridge Police Log Trends")
st.caption(
    "Counts describe reports recorded in the police log. They are not a count of verified crimes. "
    "This dashboard shows aggregate trends and does not display incident descriptions or IDs."
)


@st.cache_resource
def get_engine():
    return create_engine(database_url(), pool_pre_ping=True)


@st.cache_data(ttl=60)
def get_bounds() -> tuple[date | None, date | None, list[str]]:
    with get_engine().connect() as connection:
        bounds = connection.execute(
            text("SELECT MIN(date_time)::date, MAX(date_time)::date FROM cpd_incidents")
        ).one()
        types = connection.execute(
            text("SELECT DISTINCT type FROM cpd_incidents ORDER BY type")
        ).scalars().all()
    return bounds[0], bounds[1], list(types)


try:
    first_date, last_date, types = get_bounds()
except Exception as exc:
    st.error("The dashboard cannot read PostgreSQL yet. Start the stack and run the ETL once.")
    st.stop()

if first_date is None or last_date is None:
    st.info("No incidents have been loaded. Run the pipeline once to populate the dashboard.")
    st.stop()

selected_range = st.date_input(
    "Incident date range",
    value=(first_date, last_date),
    min_value=first_date,
    max_value=last_date,
)
if not isinstance(selected_range, (tuple, list)) or len(selected_range) != 2:
    st.info("Select both a start date and an end date.")
    st.stop()

start_date, end_date = selected_range
selected_type = st.selectbox("Report type", ["All types", *types])
filters = ["date_time >= :start_date", "date_time < :end_date"]
params: dict[str, object] = {
    "start_date": start_date,
    "end_date": end_date + timedelta(days=1),
}
if selected_type != "All types":
    filters.append("type = :incident_type")
    params["incident_type"] = selected_type
where_clause = " AND ".join(filters)


def query(sql: str) -> pd.DataFrame:
    with get_engine().connect() as connection:
        return pd.read_sql_query(text(sql), connection, params=params)


total = query(
    f"SELECT COUNT(*) AS incidents FROM cpd_incidents WHERE {where_clause}"
).iloc[0, 0]
latest = query(
    f"SELECT MAX(date_time)::date AS latest_date FROM cpd_incidents WHERE {where_clause}"
).iloc[0, 0]
monthly = query(
    "SELECT date_trunc('month', date_time)::date AS month, COUNT(*) AS incidents "
    f"FROM cpd_incidents WHERE {where_clause} GROUP BY 1 ORDER BY 1"
)
by_type = query(
    "SELECT type, COUNT(*) AS incidents FROM cpd_incidents "
    f"WHERE {where_clause} GROUP BY type ORDER BY incidents DESC LIMIT 10"
)
by_hour = query(
    "SELECT hour, COUNT(*) AS incidents FROM cpd_incidents "
    f"WHERE {where_clause} GROUP BY hour ORDER BY hour"
)
by_weekday = query(
    "SELECT weekday, COUNT(*) AS incidents FROM cpd_incidents "
    f"WHERE {where_clause} GROUP BY weekday "
    "ORDER BY MIN(EXTRACT(ISODOW FROM date_time))"
)

top_type = by_type.iloc[0]["type"] if not by_type.empty else "—"
left, middle, right = st.columns(3)
left.metric("Reports in selected range", f"{int(total):,}")
middle.metric("Most common report type", str(top_type))
right.metric("Latest report date", str(latest) if latest else "—")

chart_left, chart_right = st.columns(2)
with chart_left:
    st.subheader("Reports by month")
    if monthly.empty:
        st.info("No reports match this filter.")
    else:
        st.line_chart(monthly, x="month", y="incidents")
with chart_right:
    st.subheader("Most common report types")
    if by_type.empty:
        st.info("No reports match this filter.")
    else:
        st.bar_chart(by_type, x="type", y="incidents")

chart_left, chart_right = st.columns(2)
with chart_left:
    st.subheader("Reports by hour")
    if not by_hour.empty:
        st.bar_chart(by_hour, x="hour", y="incidents")
with chart_right:
    st.subheader("Reports by weekday")
    if not by_weekday.empty:
        st.bar_chart(by_weekday, x="weekday", y="incidents")

st.caption("Source: Cambridge Police Daily Police Log, Socrata dataset 3gki-wyrb.")
