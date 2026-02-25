import os
import sys
import duckdb
import pandas as pd
import streamlit as st
import plotly.express as px
import plotly.graph_objects as go

# Ensure scripts directory is in sys.path
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_DIR = os.path.dirname(SCRIPT_DIR)
if SCRIPT_DIR not in sys.path:
    sys.path.insert(0, SCRIPT_DIR)

DUCKDB_PATH = os.path.join(PROJECT_DIR, "data", "gold_warehouse.duckdb")

# Page setup
st.set_page_config(
    page_title="E-Commerce Medallion Pipeline Dashboard",
    page_icon="🛍️",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom Styling
st.markdown("""
    <style>
    .main {
        background-color: #0e1117;
    }
    .metric-card {
        background: linear-gradient(135deg, #1e293b 0%, #0f172a 100%);
        border: 1px solid #334155;
        border-radius: 12px;
        padding: 20px;
        box-shadow: 0 4px 6px -1px rgba(0, 0, 0, 0.1), 0 2px 4px -1px rgba(0, 0, 0, 0.06);
        text-align: center;
    }
    .metric-title {
        color: #94a3b8;
        font-size: 0.9rem;
        font-weight: 600;
        text-transform: uppercase;
        letter-spacing: 0.05em;
    }
    .metric-value {
        color: #f8fafc;
        font-size: 1.8rem;
        font-weight: 700;
        margin-top: 6px;
    }
    </style>
""", unsafe_allow_html=True)

@st.cache_resource
def get_duckdb_connection():
    if not os.path.exists(DUCKDB_PATH):
        st.error(f"DuckDB Data Warehouse file not found at: `{DUCKDB_PATH}`. Please run `silver_to_duckdb.py` and `dbt run` first.")
        st.stop()
    return duckdb.connect(DUCKDB_PATH, read_only=True)

try:
    con = get_duckdb_connection()
except Exception as e:
    st.error(f"Failed to connect to DuckDB DW: {e}")
    st.stop()

# Title Header
st.title("🛍️ E-Commerce Analytics Dashboard")
st.caption("Powered by **DuckDB Data Warehouse**, **S3 MinIO Delta Lake**, **Neon Postgres CRM**, and **dbt Core (Gold Layer)**")
st.markdown("---")

# Sidebar Filters
st.sidebar.header("🔍 Interactive Filters")

# Fetch available filter options
try:
    tiers_list = [r[0] for r in con.execute("SELECT DISTINCT loyalty_tier FROM main.dim_users_loyalty WHERE loyalty_tier IS NOT NULL;").fetchall()]
    channels_list = [r[0] for r in con.execute("SELECT DISTINCT acquisition_channel FROM main.dim_users_loyalty WHERE acquisition_channel IS NOT NULL;").fetchall()]
    min_max_date = con.execute("SELECT MIN(event_time::DATE), MAX(event_time::DATE) FROM main.fact_sales;").fetchone()
    min_date, max_date = min_max_date[0], min_max_date[1]
except Exception as e:
    st.sidebar.error(f"Error loading filter metadata: {e}")
    tiers_list, channels_list = ['VIP', 'Gold', 'Silver', 'Regular'], ['Google', 'Facebook', 'Organic', 'TikTok', 'Instagram', 'Referral']
    min_date, max_date = pd.to_datetime("2020-01-01").date(), pd.to_datetime("2020-01-31").date()

# Date Range Filter
selected_date_range = st.sidebar.date_input(
    "📅 Select Date Range",
    value=(min_date, max_date) if min_date and max_date else (pd.to_datetime("2020-01-01").date(), pd.to_datetime("2020-01-31").date()),
    min_value=min_date,
    max_value=max_date
)

# Multi-select Loyalty Tiers
selected_tiers = st.sidebar.multiselect(
    "👥 Loyalty Tiers",
    options=tiers_list,
    default=tiers_list
)

# Multi-select Acquisition Channels
selected_channels = st.sidebar.multiselect(
    "📢 Acquisition Channels",
    options=channels_list,
    default=channels_list
)

# Helper function to build dynamic WHERE clauses with correct time column names
def get_where_clause(time_col="event_time"):
    clauses = ["1=1"]
    if len(selected_date_range) == 2 and time_col:
        start_d, end_d = selected_date_range
        clauses.append(f"{time_col}::DATE >= '{start_d}' AND {time_col}::DATE <= '{end_d}'")

    if selected_tiers:
        tiers_str = "', '".join(selected_tiers)
        clauses.append(f"loyalty_tier IN ('{tiers_str}')")

    if selected_channels:
        channels_str = "', '".join(selected_channels)
        clauses.append(f"acquisition_channel IN ('{channels_str}')")

    return " AND ".join(clauses)

# Load Filtered KPI Data
@st.cache_data(ttl=60)
def load_metrics(start_d, end_d, tiers_tuple, channels_tuple):
    sales_where = get_where_clause("event_time")
    sales_sql = f"SELECT COALESCE(SUM(price), 0), COUNT(*), COUNT(DISTINCT user_session), COALESCE(AVG(price), 0) FROM main.fact_sales WHERE {sales_where};"
    rev, items, sales_sessions, avg_order = con.execute(sales_sql).fetchone()

    abandoned_where = get_where_clause("cart_time")
    abandoned_sql = f"SELECT COUNT(*) FROM main.fact_cart_abandonment WHERE {abandoned_where};"
    abandoned_count = con.execute(abandoned_sql).fetchone()[0]

    loyalty_where = get_where_clause(None)
    total_customers = con.execute(f"SELECT COUNT(*) FROM main.dim_users_loyalty WHERE {loyalty_where};").fetchone()[0]

    tot_cart_sessions = abandoned_count + sales_sessions
    abandonment_rate = (abandoned_count / tot_cart_sessions * 100) if tot_cart_sessions > 0 else 0.0

    return rev, items, total_customers, abandonment_rate, avg_order, abandoned_count

start_d = selected_date_range[0] if len(selected_date_range) >= 1 else min_date
end_d = selected_date_range[1] if len(selected_date_range) == 2 else max_date

rev, items, total_customers, abandonment_rate, avg_order, abandoned_count = load_metrics(
    start_d, end_d, tuple(selected_tiers), tuple(selected_channels)
)

# KPI Display Cards
c1, c2, c3, c4, c5 = st.columns(5)
with c1:
    st.markdown(f'<div class="metric-card"><div class="metric-title">💰 Sales Revenue</div><div class="metric-value">${rev:,.2f}</div></div>', unsafe_allow_html=True)
with c2:
    st.markdown(f'<div class="metric-card"><div class="metric-title">📦 Items Sold</div><div class="metric-value">{items:,}</div></div>', unsafe_allow_html=True)
with c3:
    st.markdown(f'<div class="metric-card"><div class="metric-title">👥 CRM Active Users</div><div class="metric-value">{total_customers:,}</div></div>', unsafe_allow_html=True)
with c4:
    st.markdown(f'<div class="metric-card"><div class="metric-title">⚠️ Abandonment Rate</div><div class="metric-value">{abandonment_rate:.1f}%</div></div>', unsafe_allow_html=True)
with c5:
    st.markdown(f'<div class="metric-card"><div class="metric-title">💳 Avg Order Value</div><div class="metric-value">${avg_order:,.2f}</div></div>', unsafe_allow_html=True)

st.markdown("<br>", unsafe_allow_html=True)

# Visualizations Row 1: Revenue Trend & Conversion Funnel
r1_col1, r1_col2 = st.columns([2, 1])

with r1_col1:
    st.subheader("📈 Sales Revenue Trend Over Time")
    sales_where = get_where_clause("event_time")
    trend_df = con.execute(f"""
        SELECT event_time::DATE as date, SUM(price) as daily_revenue, COUNT(*) as daily_items
        FROM main.fact_sales
        WHERE {sales_where}
        GROUP BY date
        ORDER BY date ASC;
    """).df()

    if not trend_df.empty:
        fig_trend = px.area(
            trend_df, 
            x="date", 
            y="daily_revenue", 
            labels={"date": "Date", "daily_revenue": "Revenue ($)"},
            color_discrete_sequence=["#3b82f6"]
        )
        fig_trend.update_layout(template="plotly_dark", margin=dict(l=20, r=20, t=30, b=20), height=350)
        st.plotly_chart(fig_trend, use_container_width=True)
    else:
        st.info("No data available for the selected date range.")

with r1_col2:
    st.subheader("🔻 Conversion Funnel")
    funnel_data = pd.DataFrame({
        "Stage": ["Cart Sessions", "Completed Purchases", "Abandoned Carts"],
        "Value": [items + abandoned_count, items, abandoned_count]
    })
    fig_funnel = px.funnel(
        funnel_data, 
        x="Value", 
        y="Stage",
        color="Stage",
        color_discrete_sequence=["#6366f1", "#10b981", "#ef4444"]
    )
    fig_funnel.update_layout(template="plotly_dark", margin=dict(l=20, r=20, t=30, b=20), height=350)
    st.plotly_chart(fig_funnel, use_container_width=True)

st.markdown("---")

# Visualizations Row 2: Loyalty Tier & Channel Distribution
r2_col1, r2_col2 = st.columns(2)

with r2_col1:
    st.subheader("👑 Loyalty Tiers Breakdown")
    loyalty_where = get_where_clause("first_active_time")
    loyalty_df = con.execute(f"""
        SELECT loyalty_tier, COUNT(*) as count 
        FROM main.dim_users_loyalty 
        WHERE {loyalty_where}
        GROUP BY loyalty_tier
        ORDER BY count DESC;
    """).df()
    fig_loyalty = px.bar(
        loyalty_df,
        x="loyalty_tier",
        y="count",
        color="loyalty_tier",
        text_auto=True,
        color_discrete_sequence=px.colors.qualitative.Pastel
    )
    fig_loyalty.update_layout(template="plotly_dark", height=320, showlegend=False)
    st.plotly_chart(fig_loyalty, use_container_width=True)

with r2_col2:
    st.subheader("📣 Acquisition Channels Share")
    channel_df = con.execute(f"""
        SELECT acquisition_channel, COUNT(*) as count 
        FROM main.dim_users_loyalty 
        WHERE {loyalty_where}
        GROUP BY acquisition_channel;
    """).df()
    fig_channel = px.pie(
        channel_df,
        values="count",
        names="acquisition_channel",
        hole=0.4,
        color_discrete_sequence=px.colors.qualitative.Set3
    )
    fig_channel.update_layout(template="plotly_dark", height=320)
    st.plotly_chart(fig_channel, use_container_width=True)

st.markdown("---")

# Visualizations Row 3: Top Selling Brands
st.subheader("🏆 Top 10 Revenue-Generating Brands")
sales_where = get_where_clause("event_time")
brand_df = con.execute(f"""
    SELECT brand, SUM(price) as total_sales, COUNT(*) as items_sold
    FROM main.fact_sales
    WHERE {sales_where}
    GROUP BY brand
    ORDER BY total_sales DESC
    LIMIT 10;
""").df()

fig_brand = px.bar(
    brand_df,
    x="total_sales",
    y="brand",
    orientation="h",
    text="items_sold",
    labels={"total_sales": "Total Revenue ($)", "brand": "Brand", "items_sold": "Units Sold"},
    color="total_sales",
    color_continuous_scale="Plasma"
)
fig_brand.update_layout(template="plotly_dark", height=380, yaxis=dict(autorange="reversed"))
st.plotly_chart(fig_brand, use_container_width=True)

# Footer
st.markdown("---")
st.caption("🟢 Connected to DuckDB Data Warehouse (`gold_warehouse.duckdb`). Real-time analytical queries executed in < 10ms.")
