"""
Oil data scraper: fetches WTI, Brent and related futures via yfinance.
"""
from __future__ import annotations

import pandas as pd

try:
    import yfinance as yf
except ImportError:
    yf = None

# Common oil-related tickers (yahoo finance)
OIL_TICKERS = {
    "WTI Crude (CL=F)": "CL=F",
    "Brent Crude (BZ=F)": "BZ=F",
    "Natural Gas (NG=F)": "NG=F",
    "RBOB Gasoline (RB=F)": "RB=F",
    "Heating Oil (HO=F)": "HO=F",
}


def get_oil_data(
    tickers: dict[str, str] | None = None,
    period: str = "1mo",
    interval: str = "1d",
) -> pd.DataFrame:
    """
    Fetch OHLCV data for oil tickers.

    Args:
        tickers: Dict of display_name -> yfinance symbol. Defaults to OIL_TICKERS.
        period: Valid period: 1d, 5d, 1mo, 3mo, 6mo, 1y, 2y, 5y.
        interval: Valid interval: 1m, 2m, 5m, 15m, 30m, 1h, 1d, 1wk, 1mo.

    Returns:
        DataFrame with MultiIndex columns (ticker, OHLCV), index = DatetimeIndex.
    """
    if yf is None:
        raise ImportError("Install yfinance: pip install yfinance")
    tickers = tickers or OIL_TICKERS
    symbols = list(tickers.values())
    data = yf.download(
        symbols,
        period=period,
        interval=interval,
        group_by="ticker",
        progress=False,
        auto_adjust=True,
    )
    if len(symbols) == 1:
        data.columns = [f"{symbols[0]}_{c}" for c in data.columns]
    return data


def get_close_for_chart(
    tickers: dict[str, str] | None = None,
    period: str = "1mo",
    interval: str = "1d",
) -> pd.DataFrame:
    """Return a DataFrame of Close prices only with simple column names for charting."""
    raw = get_oil_data(tickers=tickers, period=period, interval=interval)
    if raw.empty:
        return raw
    symbols = list((tickers or OIL_TICKERS).values())
    if len(symbols) == 1:
        close_col = [c for c in raw.columns if "Close" in str(c)]
        out = raw[close_col].copy()
        out.columns = [symbols[0]]
        return out
    # MultiIndex: (Symbol, OHLC)
    out = pd.DataFrame(index=raw.index)
    for sym in symbols:
        try:
            if isinstance(raw.columns, pd.MultiIndex):
                out[sym] = raw[sym]["Close"]
            else:
                out[sym] = raw[f"{sym}_Close"]
        except (KeyError, TypeError):
            continue
    return out


def get_latest_prices(tickers: dict[str, str] | None = None) -> pd.DataFrame:
    """Get latest price (and day change) for each oil ticker."""
    if yf is None:
        raise ImportError("Install yfinance: pip install yfinance")
    tickers = tickers or OIL_TICKERS
    rows = []
    for name, symbol in tickers.items():
        t = yf.Ticker(symbol)
        hist = t.history(period="5d")
        if hist.empty:
            rows.append({"Name": name, "Symbol": symbol, "Price": None, "Change %": None})
            continue
        last = hist.iloc[-1]
        prev = hist.iloc[-2] if len(hist) > 1 else last
        close = last["Close"]
        pct = ((close - prev["Close"]) / prev["Close"] * 100) if prev["Close"] else None
        rows.append({"Name": name, "Symbol": symbol, "Price": round(close, 2), "Change %": round(pct, 2) if pct is not None else None})
    return pd.DataFrame(rows)
