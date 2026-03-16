"""
Streamlit app: Oil data scraper dashboard.
Run: streamlit run app.py
"""
import streamlit as st
import pandas as pd
from scraper import OIL_TICKERS, get_oil_data, get_latest_prices
from db import upsert_prices, fetch_price_history

st.set_page_config(page_title="Oil Data Scraper", page_icon="🛢️", layout="wide")
st.title("🛢️ Oil Data Scraper")

# Sidebar: run scrapers button (Neon fact_prices)
st.sidebar.header("Scrapers")
if st.sidebar.button("🔄 Run spot price scraper now", type="primary"):
    try:
        from spot_price_scraper import run as run_spot_scraper
        with st.spinner("Fetching 10y of WTI/Brent/Heating Oil and saving to Neon…"):
            inserted = run_spot_scraper()
        st.sidebar.success(f"Done. Inserted/updated {inserted} rows.")
    except Exception as e:
        st.sidebar.error(f"Scraper failed: {e}")
st.sidebar.caption("Writes to Neon fact_prices. May take 1–2 min.")

live_tab, history_tab = st.tabs(["📈 Live market", "🗄️ Stored history (Neon)"])

with live_tab:
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

with history_tab:
    st.subheader("Stored price history (Neon)")
    try:
        hist_df = fetch_price_history(limit=1000)
    except Exception as e:
        st.error(f"Error reading from Neon: {e}")
    else:
        if hist_df.empty:
            st.info("No stored price history yet. Visit the Live tab to fetch and store prices.")
        else:
            symbols = sorted(hist_df["symbol"].unique())
            sel_symbols = st.multiselect("Filter by symbol", symbols, default=symbols)
            df_filtered = hist_df[hist_df["symbol"].isin(sel_symbols)] if sel_symbols else hist_df

            st.dataframe(df_filtered, width="stretch", hide_index=True)

            # Simple chart: one line per symbol over price_date
            pivot = df_filtered.pivot_table(
                index="price_date",
                columns="symbol",
                values="close_price",
                aggfunc="last",
            ).sort_index()
            if not pivot.empty:
                st.line_chart(pivot)
