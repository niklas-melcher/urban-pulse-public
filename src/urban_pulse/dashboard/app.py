from datetime import datetime

import duckdb
import pandas as pd
import plotly.express as px
import streamlit as st

from urban_pulse.models.settings import Settings

st.set_page_config(page_title="Urban Pulse — Amsterdam", layout="wide")


@st.cache_resource  # type: ignore[untyped-decorator]
def get_conn() -> duckdb.DuckDBPyConnection:
    """cache_resource: create this ONCE per app session. Read-only —
    the dashboard never writes, only ingestion does, and DuckDB only
    allows one read-write connection to a file at a time (see ADR 0005).
    Assumes schema already exists; run ingestion at least once first."""
    settings = Settings()
    return duckdb.connect(settings.duckdb_path, read_only=True)


@st.cache_data(ttl=60)  # type: ignore[untyped-decorator]
def load_df(_conn: duckdb.DuckDBPyConnection, query: str) -> pd.DataFrame:
    """cache_data: cache query RESULTS for 60s, so switching tabs
    doesn't re-hit DuckDB every time. Leading underscore on _conn tells
    Streamlit "don't try to hash this argument for cache-key purposes"
    — connections aren't hashable/comparable in a meaningful way."""
    return _conn.execute(query).fetchdf()


conn = get_conn()

st.title("🚲 Urban Pulse — Amsterdam")
st.caption(f"Cycling & air quality overview · refreshed {datetime.now():%Y-%m-%d %H:%M}")
# --- KPI row: latest snapshot per segment ---
st.subheader("Latest cycling counts")
latest_cycling = load_df(conn, "SELECT * FROM gold_cycling_latest ORDER BY segment_id")

cols = st.columns(len(latest_cycling) or 1)
for col, (_, row) in zip(cols, latest_cycling.iterrows(), strict=False):
    with col:
        uptime_pct = row["uptime"] * 100
        # surface data-confidence, not just the raw numbers — a segment
        # with low uptime is extrapolated, not directly measured
        confidence = "🟢" if uptime_pct > 90 else "🟡" if uptime_pct > 50 else "🔴"
        st.metric(
            label=f"Segment {row['segment_id']} {confidence}",
            value=f"{row['bike_count']:.0f} bikes/hr",
            delta=f"uptime {uptime_pct:.0f}%",
            delta_color="off",
        )
        st.caption(
            f"🚶 {row['pedestrian_count']:.0f}  🚗 {row['car_count']:.0f}  "
            f"🚛 {row['heavy_count']:.0f}"
        )

st.caption(
    "🟢 uptime > 90%  ·  🟡 uptime 50–90% (partially extrapolated)  ·  "
    "🔴 uptime < 50% (mostly extrapolated)"
)
st.divider()

# --- Cycling trend ---
st.subheader("Cycling trends (daily totals)")
cycling_daily = load_df(
    conn,
    """
    SELECT segment_id, day, total_bikes, total_pedestrians, total_cars
    FROM gold_cycling_daily
    ORDER BY day
    """,
)
if not cycling_daily.empty:
    metric = st.radio("Metric", ["total_bikes", "total_pedestrians", "total_cars"], horizontal=True)
    fig = px.line(
        cycling_daily,
        x="day",
        y=metric,
        color="segment_id",
        markers=True,
        title=f"Daily {metric.replace('total_', '')} by segment",
    )
    st.plotly_chart(fig, use_container_width=True)
else:
    st.info("No cycling data yet.")

st.divider()

# --- Air quality trend ---
st.subheader("Air quality trends (daily average)")
air_daily = load_df(
    conn,
    "SELECT station_number, pollutant, day, avg_value FROM gold_air_quality_daily ORDER BY day",
)
if not air_daily.empty:
    pollutants = sorted(air_daily["pollutant"].unique())
    selected = st.selectbox(
        "Pollutant", pollutants, index=pollutants.index("NO2") if "NO2" in pollutants else 0
    )
    filtered = air_daily[air_daily["pollutant"] == selected]
    fig2 = px.line(
        filtered,
        x="day",
        y="avg_value",
        color="station_number",
        markers=True,
        title=f"Daily average {selected} by station",
    )
    st.plotly_chart(fig2, use_container_width=True)
else:
    st.info("No air quality data yet.")
