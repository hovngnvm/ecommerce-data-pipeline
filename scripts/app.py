import os
import sys
import psycopg2
import pandas as pd
import streamlit as st
import plotly.express as px

# Ensure scripts directory is in sys.path
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
if SCRIPT_DIR not in sys.path:
    sys.path.insert(0, SCRIPT_DIR)

from utils.config import NEON_DB_HOST, NEON_DB_USER, NEON_DB_PASSWORD, NEON_DB_NAME, NEON_DB_PORT

# Set page config
st.set_page_config(
    page_title="E-Commerce Medallion Pipeline Dashboard",
    page_icon="🛍️",
    layout="wide"
)

# Load environment variables using utils.config and cache resource
@st.cache_resource
def get_db_connection():
    conn = psycopg2.connect(
        host=NEON_DB_HOST,
        user=NEON_DB_USER,
        password=NEON_DB_PASSWORD,
        database=NEON_DB_NAME,
        port=NEON_DB_PORT,
        sslmode="require"
    )
    return conn

try:
    conn = get_db_connection()
except Exception as e:
    st.error(f"Failed to connect to Neon Postgres: {e}")
    st.stop()

# Title
st.title("🛍️ E-Commerce Analytics Dashboard")
st.markdown("Metrics & Insights powered by Neon Postgres, PySpark, and dbt Gold Layer Tables.")
st.markdown("---")

# Queries
@st.cache_data(ttl=60)
def load_kpis():
    cursor = conn.cursor()
    # 1. Total Sales Revenue & Items Sold
    cursor.execute("SELECT COALESCE(SUM(price), 0), COUNT(*) FROM public.fact_sales;")
    revenue, items_sold = cursor.fetchone()
    
    # 2. Total Customers
    cursor.execute("SELECT COUNT(*) FROM public.dim_users_loyalty;")
    total_customers = cursor.fetchone()[0]

    # 3. Cart Abandonment Rate
    # Rate = Abandoned Carts / (Abandoned Carts + Completed Sales Carts)
    cursor.execute("SELECT COUNT(*) FROM public.fact_cart_abandonment;")
    abandoned_count = cursor.fetchone()[0]
    
    cursor.execute("SELECT COUNT(DISTINCT user_session) FROM public.fact_sales;")
    purchased_sessions = cursor.fetchone()[0]
    
    total_cart_sessions = abandoned_count + purchased_sessions
    abandonment_rate = (abandoned_count / total_cart_sessions * 100) if total_cart_sessions > 0 else 0.0
    
    cursor.close()
    return revenue, items_sold, total_customers, abandonment_rate

@st.cache_data(ttl=60)
def load_charts_data():
    # 1. Loyalty distribution
    loyalty_df = pd.read_sql("SELECT loyalty_tier, COUNT(*) as count FROM public.dim_users_loyalty GROUP BY loyalty_tier;", conn)
    
    # 2. Acquisition Channels
    channel_df = pd.read_sql("SELECT acquisition_channel, COUNT(*) as count FROM public.dim_users_loyalty GROUP BY acquisition_channel;", conn)
    
    # 3. Top Brands Sales
    brand_df = pd.read_sql("SELECT brand, SUM(price) as sales, COUNT(*) as volume FROM public.fact_sales GROUP BY brand ORDER BY sales DESC LIMIT 10;", conn)
    
    return loyalty_df, channel_df, brand_df

# Load data
with st.spinner("Fetching latest data from Neon Cloud..."):
    revenue, items_sold, total_customers, abandonment_rate = load_kpis()
    loyalty_df, channel_df, brand_df = load_charts_data()

# KPI Display Row
col1, col2, col3, col4 = st.columns(4)
with col1:
    st.metric(label="💰 Total Sales Revenue", value=f"${revenue:,.2f}")
with col2:
    st.metric(label="📦 Total Items Sold", value=f"{items_sold:,}")
with col3:
    st.metric(label="👥 Active Customers", value=f"{total_customers:,}")
with col4:
    st.metric(label="⚠️ Cart Abandonment Rate", value=f"{abandonment_rate:.2f}%")

st.markdown("---")

# Visualizations Row 1
chart_col1, chart_col2 = st.columns(2)

with chart_col1:
    st.subheader("👥 Loyalty Tiers Distribution")
    fig_loyalty = px.bar(
        loyalty_df, 
        x="loyalty_tier", 
        y="count", 
        color="loyalty_tier",
        labels={"loyalty_tier": "Loyalty Tier", "count": "Number of Users"},
        color_discrete_sequence=px.colors.qualitative.Pastel
    )
    st.plotly_chart(fig_loyalty, use_container_width=True)

with chart_col2:
    st.subheader("📢 Acquisition Channels")
    fig_channel = px.pie(
        channel_df, 
        values="count", 
        names="acquisition_channel",
        color_discrete_sequence=px.colors.qualitative.Set3
    )
    st.plotly_chart(fig_channel, use_container_width=True)

st.markdown("---")

# Visualizations Row 2
st.subheader("🔥 Top 10 Selling Brands")
fig_brand = px.bar(
    brand_df, 
    x="brand", 
    y="sales", 
    text="volume",
    labels={"brand": "Brand", "sales": "Total Sales ($)", "volume": "Items Sold"},
    color="sales",
    color_continuous_scale="Viridis"
)
st.plotly_chart(fig_brand, use_container_width=True)

# Footer
st.markdown("---")
st.caption("Dashboard refreshed automatically every 60 seconds. Connected to Singapore AWS (Neon).")
