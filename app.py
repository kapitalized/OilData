"""
Streamlit app: Oil data scraper dashboard.
Run: streamlit run app.py
"""
import streamlit as st
import pandas as pd
from scraper import OIL_TICKERS, get_oil_data, get_latest_prices
from db import upsert_prices

st.set_page_config(page_title="Oil Data Scraper", page_icon="🛢️", layout="wide")
st.title("🛢️ Oil Data Scraper")

# Sidebar: options
st.sidebar.header("Options")
period = st.sidebar.selectbox("Period", ["5d", "1mo", "3mo", "6mo", "1y"], index=1)
interval = st.sidebar.selectbox("Interval", ["1d", "1h"], index=0)
ticker_labels = list(OIL_TICKERS.keys())
selected = st.sidebar.multiselect("Tickers", ticker_labels, default=ticker_labels[:2])
tickers = {k: OIL_TICKERS[k] for k in selected} if selected else OIL_TICKERS

# Latest prices
st.subheader("Latest prices")
try:
    latest = get_latest_prices(tickers)
    st.dataframe(latest, width="stretch", hide_index=True)
    # Persist latest snapshot to Neon (best-effort)
    try:
        upsert_prices(latest)
    except Exception as db_err:
        st.warning(f"Could not save to Neon: {db_err}")
except Exception as e:
    st.error(f"Error fetching latest prices: {e}")

# Historical data
st.subheader("Historical data")
if not tickers:
    st.warning("Select at least one ticker in the sidebar.")
else:
    try:
        df = get_oil_data(tickers=tickers, period=period, interval=interval)
        if df.empty:
            st.warning("No data returned for the selected period/interval.")
        else:
            # Build close-only df for chart from same data
            symbols = list(tickers.values())
            chart_df = pd.DataFrame(index=df.index)
            for sym in symbols:
                if isinstance(df.columns, pd.MultiIndex) and sym in df.columns:
                    chart_df[sym] = df[sym]["Close"]
                else:
                    c = f"{sym}_Close"
                    if c in df.columns:
                        chart_df[sym] = df[c]
            if not chart_df.empty:
                st.line_chart(chart_df)
            with st.expander("View raw data"):
                st.dataframe(df, width="stretch", hide_index=True)
    except Exception as e:
        st.error(f"Error fetching historical data: {e}")
