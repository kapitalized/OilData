"""
Spot Price Scraper (Historic & Daily) for Neon fact_prices.

Usage (from OilData folder):
    python spot_price_scraper.py
"""
from __future__ import annotations

import time
from datetime import datetime, timedelta

import yfinance as yf

from db import insert_fact_prices_with_log


TICKERS = {
    "CL=F": {
        "oil_type_name": "Light Sweet Crude",
        "oil_type_code": "LIGHT_SWEET",
        "market_location": "WTI Cushing",
    },
    "BZ=F": {
        "oil_type_name": "Light Sweet Crude",
        "oil_type_code": "BRENT_LIGHT",
        "market_location": "Brent Dated",
    },
    "HO=F": {
        "oil_type_name": "Diesel",
        "oil_type_code": "HEATING_OIL",
        "market_location": "NY Harbor Heating Oil",
    },
}


def fetch_with_backoff(symbol: str, period: str, interval: str, max_retries: int = 5):
    delay = 1.0
    last_exc: Exception | None = None
    for _ in range(max_retries):
        try:
            return yf.download(
                symbol,
                period=period,
                interval=interval,
                auto_adjust=True,
                progress=False,
            )
        except Exception as exc:  # noqa: BLE001
            last_exc = exc
            time.sleep(delay)
            delay *= 2
    if last_exc:
        raise last_exc
    raise RuntimeError("Unknown error in fetch_with_backoff")


def build_price_records(symbol: str, df) -> list[dict]:
    meta = TICKERS[symbol]
    records: list[dict] = []
    if df is None or df.empty:
        return records

    for idx, row in df.iterrows():
        price_date = idx.date() if isinstance(idx, datetime) else idx
        close_val = row.get("Close")
        if close_val is None:
            continue
        records.append(
            {
                "oil_type_name": meta["oil_type_name"],
                "oil_type_code": meta["oil_type_code"],
                "market_location": meta["market_location"],
                "price_date": price_date,
                "price_usd_per_bbl": float(close_val),
            }
        )
    return records


def run():
    scraper_name = "spot_price_scraper_yfinance"
    src_url = "https://query1.finance.yahoo.com"

    # 10 years of daily data
    all_records: list[dict] = []
    for symbol in TICKERS:
        df = fetch_with_backoff(symbol, period="10y", interval="1d")
        all_records.extend(build_price_records(symbol, df))

    inserted = insert_fact_prices_with_log(
        scraper_name=scraper_name,
        src_url=src_url,
        records=all_records,
    )
    print(f"Inserted/updated {inserted} fact_prices rows")


if __name__ == "__main__":
    run()

