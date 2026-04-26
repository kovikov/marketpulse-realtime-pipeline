import os

import pandas as pd
import plotly.express as px
import psycopg2
import streamlit as st
from dotenv import load_dotenv

load_dotenv()

POSTGRES_HOST = os.getenv("POSTGRES_HOST", "localhost")
POSTGRES_PORT = os.getenv("POSTGRES_PORT", "5432")
POSTGRES_DB = os.getenv("POSTGRES_DB", "stock_market")
POSTGRES_USER = os.getenv("POSTGRES_USER", "marketpulse")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "marketpulse123")


def create_database_connection():
    connection = psycopg2.connect(
        host=POSTGRES_HOST,
        port=POSTGRES_PORT,
        database=POSTGRES_DB,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD,
    )
    return connection


def load_stock_data():
    connection = create_database_connection()

    query = """
        SELECT
            symbol,
            price,
            open_price,
            high_price,
            low_price,
            volume,
            event_time,
            inserted_at
        FROM stock_prices
        ORDER BY inserted_at DESC
        LIMIT 1000;
    """

    df = pd.read_sql(query, connection)
    connection.close()

    return df


st.set_page_config(
    page_title="MarketPulse Real-Time Stock Dashboard",
    layout="wide",
)

st.title("MarketPulse Real-Time Stock Market Insights")

st.caption("Live stock data pipeline using Python, Kafka, PostgreSQL, and Streamlit.")

refresh_seconds = st.sidebar.slider(
    "Dashboard refresh interval in seconds",
    min_value=5,
    max_value=60,
    value=10,
)

st.sidebar.write("Refresh the browser page to update the latest data.")

try:
    df = load_stock_data()

    if df.empty:
        st.warning("No stock data found yet. Start producer.py and consumer.py first.")

    else:
        df["event_time"] = pd.to_datetime(df["event_time"])
        df = df.sort_values("event_time")

        latest_df = df.sort_values("event_time").groupby("symbol").tail(1)

        col1, col2, col3 = st.columns(3)

        with col1:
            st.metric("Tracked Symbols", latest_df["symbol"].nunique())

        with col2:
            st.metric("Latest Records", len(df))

        with col3:
            st.metric("Total Volume", int(latest_df["volume"].sum()))

        st.subheader("Latest Price by Symbol")

        price_chart = px.line(
            df,
            x="event_time",
            y="price",
            color="symbol",
            markers=True,
            title="Stock Price Movement",
        )

        st.plotly_chart(price_chart, use_container_width=True)

        st.subheader("Latest Trading Volume")

        volume_chart = px.bar(
            latest_df,
            x="symbol",
            y="volume",
            color="symbol",
            title="Latest Volume by Stock",
        )

        st.plotly_chart(volume_chart, use_container_width=True)

        st.subheader("Latest Stock Data")

        st.dataframe(
            latest_df[
                [
                    "symbol",
                    "price",
                    "open_price",
                    "high_price",
                    "low_price",
                    "volume",
                    "event_time",
                ]
            ],
            use_container_width=True,
        )

except Exception as error:
    st.error("Dashboard error.")
    st.write(error)
