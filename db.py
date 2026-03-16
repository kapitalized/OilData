"""
Neon database helper for storing latest oil prices.
"""
from __future__ import annotations

import os
from datetime import date
import json

import pandas as pd
import psycopg2
from psycopg2.extras import execute_batch, RealDictCursor


NEON_DATABASE_URL = os.environ.get("NEON_DATABASE_URL")


def get_conn():
    if not NEON_DATABASE_URL:
        raise RuntimeError("NEON_DATABASE_URL environment variable is not set")
    return psycopg2.connect(NEON_DATABASE_URL)


def _ensure_schema(cur) -> None:
    """
    Create minimal app tables if they don't exist.
    This is intentionally small and focused on yfinance price storage only.
    """
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS dim_oil_types_app (
            id          SERIAL PRIMARY KEY,
            code        TEXT UNIQUE NOT NULL,
            name        TEXT NOT NULL
        );
        """
    )
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS fact_prices_app (
            id              BIGSERIAL PRIMARY KEY,
            oil_type_id     INT NOT NULL REFERENCES dim_oil_types_app(id),
            price_date      DATE NOT NULL,
            close_price     NUMERIC(18, 6) NOT NULL,
            change_percent  NUMERIC(10, 4),
            src             TEXT NOT NULL,
            created_at      TIMESTAMPTZ DEFAULT now(),
            UNIQUE (oil_type_id, price_date, src)
        );
        """
    )

    # Ensure core analytical tables also exist (id-based dim_oil_types already matches Neon)
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS dim_countries (
            iso_code     VARCHAR(3) PRIMARY KEY,
            country_name TEXT NOT NULL,
            region       TEXT
        );
        """
    )
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS fact_prices (
            price_id          SERIAL PRIMARY KEY,
            oil_type_id       INTEGER REFERENCES dim_oil_types(id),
            price_usd_per_bbl DECIMAL(10,2),
            market_location   TEXT,
            price_date        DATE NOT NULL,
            UNIQUE (oil_type_id, price_date, market_location)
        );
        """
    )
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS src_scraper_logs (
            log_id           SERIAL PRIMARY KEY,
            scraper_name     TEXT NOT NULL,
            run_time         TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            rows_inserted    INTEGER,
            status           TEXT,
            error_message    TEXT,
            raw_response_json JSONB
        );
        """
    )


def upsert_prices(df) -> None:
    """
    Store latest prices into Neon.

    Expects a DataFrame with columns:
      - Name
      - Symbol
      - Price
      - Change %
    """
    if df is None or df.empty:
        return

    rows = []
    for _, row in df.iterrows():
        rows.append(
            (
                row.get("Symbol"),
                row.get("Name") or row.get("Symbol"),
                row.get("Price"),
                row.get("Change %"),
            )
        )

    with get_conn() as conn:
        with conn.cursor() as cur:
            _ensure_schema(cur)

            # Load existing oil types (app table)
            cur.execute("SELECT code, id FROM dim_oil_types_app")
            existing = {code: oid for code, oid in cur.fetchall()}

            # Insert any new oil types we see
            for code, name, _price, _chg in rows:
                if not code or code in existing:
                    continue
                cur.execute(
                    """
                    INSERT INTO dim_oil_types_app (code, name)
                    VALUES (%s, %s)
                    ON CONFLICT (code) DO NOTHING
                    RETURNING id;
                    """,
                    (code, name or code),
                )
                res = cur.fetchone()
                if res:
                    existing[code] = res[0]

            today = date.today()
            data_for_insert = []
            for code, _name, price, change_pct in rows:
                oil_type_id = existing.get(code)
                if oil_type_id is None or price is None:
                    continue
                data_for_insert.append(
                    (oil_type_id, today, price, change_pct, "yfinance")
                )

            if not data_for_insert:
                return

            execute_batch(
                cur,
                """
                INSERT INTO fact_prices_app (
                    oil_type_id,
                    price_date,
                    close_price,
                    change_percent,
                    src
                )
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (oil_type_id, price_date, src) DO NOTHING;
                """,
                data_for_insert,
            )


def fetch_price_history(limit: int = 500) -> pd.DataFrame:
    """
    Return recent price history from Neon joined with dim_oil_types.
    """
    with get_conn() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            _ensure_schema(cur)
            cur.execute(
                """
                SELECT
                    fp.price_date,
                    dt.code AS symbol,
                    dt.name AS name,
                    fp.close_price,
                    fp.change_percent,
                    fp.src,
                    fp.created_at
                FROM fact_prices_app fp
                JOIN dim_oil_types_app dt ON dt.id = fp.oil_type_id
                ORDER BY fp.price_date DESC, dt.code
                LIMIT %s;
                """,
                (limit,),
            )
            rows = cur.fetchall()
    if not rows:
        return pd.DataFrame()
    return pd.DataFrame(rows)


def get_or_create_oil_type(name: str, code: str | None = None) -> int:
    """
    Helper for analytical dim_oil_types.
    """
    if not name:
        raise ValueError("oil type name is required")
    code = code or name.upper().replace(" ", "_")

    with get_conn() as conn:
        with conn.cursor() as cur:
            _ensure_schema(cur)
            cur.execute("SELECT id FROM dim_oil_types WHERE name = %s", (name,))
            row = cur.fetchone()
            if row:
                return row[0]
            cur.execute(
                "INSERT INTO dim_oil_types (code, name) VALUES (%s, %s) RETURNING id;",
                (code, name),
            )
            new_id = cur.fetchone()[0]
    return new_id


def insert_fact_prices_with_log(
    scraper_name: str,
    src_url: str,
    records: list[dict],
) -> int:
    """
    Insert a batch of analytical price records and create a scraper log.

    Each record dict must have:
      - oil_type_name
      - oil_type_code (optional)
      - market_location
      - price_date (datetime.date)
      - price_usd_per_bbl
    """
    if not records:
        return 0

    with get_conn() as conn:
        with conn.cursor() as cur:
            _ensure_schema(cur)

            cur.execute(
                """
                INSERT INTO src_scraper_logs (scraper_name, rows_inserted, status, raw_response_json)
                VALUES (%s, %s, %s, %s)
                RETURNING log_id;
                """,
                (scraper_name, 0, "running", json.dumps(records, default=str)),
            )
            log_id = cur.fetchone()[0]

            inserted = 0
            for rec in records:
                oil_type_name = rec["oil_type_name"]
                oil_type_code = rec.get("oil_type_code")
                market_location = rec.get("market_location")
                price_date = rec["price_date"]
                price = rec["price_usd_per_bbl"]

                oil_type_id = get_or_create_oil_type(
                    name=oil_type_name,
                    code=oil_type_code,
                )

                cur.execute(
                    """
                    INSERT INTO fact_prices (
                        oil_type_id,
                        price_usd_per_bbl,
                        market_location,
                        price_date
                    )
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (oil_type_id, price_date, market_location)
                    DO UPDATE SET price_usd_per_bbl = EXCLUDED.price_usd_per_bbl;
                    """,
                    (oil_type_id, price, market_location, price_date),
                )
                inserted += 1

            cur.execute(
                """
                UPDATE src_scraper_logs
                SET rows_inserted = %s, status = %s
                WHERE log_id = %s;
                """,
                (inserted, "success", log_id),
            )

    return inserted

